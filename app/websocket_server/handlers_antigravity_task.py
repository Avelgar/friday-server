import os
import re
import json
import time
import asyncio
import logging
import urllib.parse
import aiomysql

from app.database.connection import get_async_db_connection
from app.services.ai_service import ai_instance
from app.services.ai.router import (
    EmptyPlanError,
    summarize_planner_failure,
    _inject_sync_tag,
    _strip_run_command,
    extract_mentioned_files,
)
from app.services.ai.antigravity import AG_MAX_TOTAL_TOKENS
from app.websocket_server.utils import async_send
from app.websocket_server.antigravity_storage import (
    partition_get_files,
    antigravity_user_sockets,
    active_antigravity_tasks,
    get_least_used_key_index,
    send_antigravity_event,
    load_chat_state,
    save_chat_state,
    UPLOADS_DIR,
    detect_requested_ide_tag,
    detect_run_command_urls,
    build_synced_meta,
    build_server_file_manifest,
)

logger = logging.getLogger("WS_Server")

# Сколько раз в одном чате перехватывать бесполезные curl-команды, прежде чем
# просто отдать их IDE как есть (защита от бесконечного цикла перехватов).
CURL_GUARD_LIMIT = 2

# Бюджет на ВЕСЬ план в одном сообщении. Квота аккаунта — 250K, поэтому план
# ограничен ею целиком, а не «250K на каждый шаг».
PLAN_TOKEN_BUDGET = 250_000
# Меньше этого запускать шаг бессмысленно: агент только скачает файл и встанет.
AG_MIN_STEP_TOKENS = 25_000


def _apply_direct_edits(chat_uid: str, edits: dict) -> dict:
    """
    Записывает новые версии файлов, которые DIRECT поправил без песочницы,
    и возвращает {путь: url} — тот же формат, что отдаёт Antigravity после [SYNC].
    """
    applied = {}
    if not edits:
        return applied
    safe_uid = os.path.basename(chat_uid or "common")
    base_dir = os.path.abspath(os.path.join(UPLOADS_DIR, safe_uid))
    for rel_p, content in edits.items():
        clean = re.sub(r"^(\./)+", "", str(rel_p).replace("\\", "/")).lstrip("/")
        if not clean or ".." in clean.split("/"):
            continue
        dest = os.path.abspath(os.path.join(base_dir, clean))
        if not dest.startswith(base_dir + os.sep):
            continue
        try:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "w", encoding="utf-8") as fh:
                fh.write(content)
            applied[clean] = (
                f"https://friday-assistant.ru/uploads/{safe_uid}/"
                f"{urllib.parse.quote(clean)}?v={int(time.time())}"
            )
        except Exception as e:
            logger.warning(f"⚠️ [DIRECT EDIT WRITE] {clean}: {e}")
    return applied


# Короткие ответы на «Запустить выполнение этого плана?»
_PLAN_APPROVAL_RE = re.compile(
    r"^(?:да|ага|ок|ok|окей|давай|поехали|старт|запускай|запускай план|выполняй|выполнить(?: план)?|"
    r"выполни(?: план)?|да,? (?:выполняй|запускай|выполнить|давай)|"
    r"да,? (?:выполнить|запустить) план\.?|запустить план|погнали|го)$",
    re.IGNORECASE,
)


def _is_plan_answer(prompt: str) -> bool:
    """
    Ответ на план — это КОРОТКАЯ реплика («да», «отмена», «выполняй»).

    Без этой проверки любое сообщение, пока pending_plan висит в состоянии,
    попадало в ветку выполнения плана: «Давай доделывай, что-то ты остановился»
    содержало «да» → план выполнялся повторно (шаги пустые) → «Готово.».
    """
    t = (prompt or "").strip()
    if not t or len(t) > 60 or len(t.split()) > 5:
        return False
    low = t.lower().rstrip(".!")
    if _PLAN_APPROVAL_RE.match(low):
        return True
    return any(w in low for w in ("отмен", "cancel", "не надо", "стоп")) or low in ("нет", "no")


def _is_plan_approval(ans_low: str) -> bool:
    return bool(_PLAN_APPROVAL_RE.match((ans_low or "").strip().rstrip(".!")))


def _comma_name_error(paths: list) -> str:
    """Файл(ы) с запятой в имени: протокол GET_FILES их не передаёт."""
    names = "**, **".join(paths)
    return (
        "⚠️ Не могу запросить файл(ы) с ПК: **" + names + "**\n\n"
        "В имени есть **запятая**, а список файлов в запросе разделяется именно "
        "запятыми — IDE получит два обрезанных имени и не найдёт их.\n\n"
        "Переименуй файл без запятой (например через `_`) и повтори задачу."
    )


def _summary_need_files(plan: dict, project_tree, manifest_paths) -> list:
    """
    Файлы, которые планировщик перечислил в прозе вместо "need_files".

    Такой план (steps=[]) без этого превращался в ответ пользователю
    «необходимо проверить такие-то файлы» вместо запроса файлов у IDE.
    """
    if not isinstance(plan, dict) or (plan.get("steps") or []):
        return []
    text = f"{plan.get('summary') or ''} {plan.get('need_reason') or ''}".strip()
    if not text:
        return []
    known = set(manifest_paths or set())
    known |= {str(nf).strip() for nf in (plan.get("need_files") or [])}
    return [p for p in extract_mentioned_files(text, project_tree) if p not in known]


def _manifest_has_url(file_manifest, url: str) -> bool:
    """URL уже относится к файлу, который лежит на сервере (т.е. скачивать его не надо)."""
    if not url or not file_manifest:
        return False
    clean = url.split("?")[0].rstrip("/")
    for f in file_manifest:
        fu = str(f.get("url") or "").split("?")[0].rstrip("/")
        if fu and fu == clean:
            return True
    return False


def _format_plan_message(plan: dict) -> str:
    summary = (plan.get("summary") or "").strip()
    steps = plan.get("steps") or []
    lines = ["📋 **План выполнения:**"]
    if summary:
        lines.append(summary)
    lines.append("")
    for idx, st in enumerate(steps, start=1):
        f_name = st.get("file") or "новый файл"
        lr = (st.get("line_ranges") or "").strip()
        lr_str = f" (строки `{lr}`)" if lr else ""
        title = (st.get("title") or "").strip()
        instr = (st.get("instruction") or "").strip()
        head = f"**{idx}. `{f_name}`{lr_str}**" + (f" — {title}" if title else "")
        lines.append(head)
        if instr:
            lines.append(f"   {instr}")
    lines.append("")
    lines.append("[ASK_USER: Запустить выполнение этого плана? | Выполнить план | Отмена]")
    return "\n".join(lines)


async def handle_antigravity_cancel_task(websocket, data):
    chat_uid = data.get("chat_id")
    ui_msg_id = data.get("ui_msg_id")
    logger.info(f"🛑 [WS CANCEL REQUEST] Запрос отмены для чата: {chat_uid}")

    if chat_uid and chat_uid in active_antigravity_tasks:
        task = active_antigravity_tasks[chat_uid]
        if not task.done():
            task.cancel()
            logger.info(f"🛑 [WS CANCEL] Таск успешно отменен в памяти: {chat_uid}")

    await async_send(websocket, {
        "type": "antigravity_cancelled",
        "chat_id": chat_uid,
        "ui_msg_id": ui_msg_id
    })


async def handle_antigravity_task(websocket, data):
    prompt = data.get("prompt", "").strip()
    is_tool_response = bool(data.get("is_tool_response", False))
    db_summary = (data.get("db_summary") or "").strip()
    target_files = data.get("target_files", [])
    user_token = data.get("user_token")
    workspace_path = data.get("workspace_path")
    chat_uid = data.get("chat_id") or data.get("chat_uid") or getattr(websocket, "active_chat_uid", None)

    if user_token:
        antigravity_user_sockets[user_token] = websocket
    if not chat_uid:
        chat_uid = f"chat_{int(time.time() * 1000)}"
    websocket.active_chat_uid = chat_uid

    ui_msg_id = data.get("ui_msg_id", str(int(time.time() * 1000)))
    if not prompt:
        return

    logger.info(f"📥 [WS IN] Чат: {chat_uid} | Роль: {'tool' if is_tool_response else 'user'} | Задача: '{prompt[:70]}' | ui_msg_id: {ui_msg_id}")

    chat_state = load_chat_state(chat_uid)
    if data.get("project_tree"):
        chat_state["project_tree"] = str(data["project_tree"])[:10000]
        chat_state["tree_updated_at"] = time.time()
        save_chat_state(chat_uid, chat_state)

    if not is_tool_response:
        clean_title_text = re.sub(r"\[Прикреплен.*?\]\s*Ссылка:.*?\n\n", "", prompt, flags=re.DOTALL).strip()
        if not clean_title_text:
            clean_title_text = prompt
        auto_title = clean_title_text[:30].replace("\n", " ").strip()
        if len(clean_title_text) > 30:
            auto_title += "..."
    else:
        auto_title = "Новый диалог"

    msg_role = "tool" if is_tool_response else "user"
    last_tag = chat_state.get("last_requested_tag")

    if is_tool_response and last_tag == "GET_TREE":
        chat_state["project_tree"] = prompt[:10000]
        chat_state["tree_updated_at"] = time.time()
        chat_state["last_requested_tag"] = None
        save_chat_state(chat_uid, chat_state)
        text_for_db = "[Дерево проекта обновлено и сохранено на сервере]"
    else:
        if is_tool_response:
            chat_state["last_requested_tag"] = None
            save_chat_state(chat_uid, chat_state)
        text_for_db = db_summary if (is_tool_response and db_summary) else prompt

    conn = None; cursor = None
    assigned_key = None
    last_engine = None
    need_title_update = False
    recent_history = []
    synced_files_dict = {}

    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)

        await cursor.execute(
            "SELECT key_index, title, last_engine, synced_files FROM antigravity_chats WHERE chat_uid = %s",
            (chat_uid,)
        )
        chat_row = await cursor.fetchone()

        if chat_row:
            assigned_key = chat_row["key_index"]
            last_engine = chat_row.get("last_engine")
            if chat_row.get("title") in ("Новый диалог", None, "") and not is_tool_response:
                need_title_update = True
            if chat_row.get("synced_files"):
                try:
                    synced_files_dict = (
                        json.loads(chat_row["synced_files"])
                        if isinstance(chat_row["synced_files"], str)
                        else dict(chat_row["synced_files"])
                    )
                except Exception:
                    synced_files_dict = {}
        else:
            assigned_key = await get_least_used_key_index(cursor)
            # ON DUPLICATE KEY: два параллельных запроса (реконнект, retry клиента)
            # не должны падать на «Duplicate entry ... for key 'chat_uid'».
            await cursor.execute(
                """INSERT INTO antigravity_chats (chat_uid, title, key_index, user_token, workspace_path) 
                   VALUES (%s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE chat_uid = chat_uid""",
                (chat_uid, auto_title, assigned_key, user_token, workspace_path)
            )
            await conn.commit()

        await cursor.execute(
            "SELECT role, text FROM antigravity_messages WHERE chat_uid = %s ORDER BY id DESC LIMIT 12",
            (chat_uid,)
        )
        recent_history = list(reversed(await cursor.fetchall() or []))

        await cursor.execute(
            "INSERT INTO antigravity_messages (chat_uid, role, text) VALUES (%s, %s, %s)",
            (chat_uid, msg_role, text_for_db)
        )
        await conn.commit()

    except Exception as dbe:
        logger.error(f"[DB Error Before Task] {dbe}", exc_info=True)
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()

    async def send_status_update(status_payload):
        await send_antigravity_event(user_token, websocket, {
            "type": "antigravity_status",
            "ui_msg_id": ui_msg_id,
            "chat_id": chat_uid,
            **status_payload
        })

    async def send_chunk_update(delta: str):
        if not delta:
            return
        await send_antigravity_event(user_token, websocket, {
            "type": "antigravity_chunk",
            "ui_msg_id": ui_msg_id,
            "chat_id": chat_uid,
            "delta": delta
        })

    active_antigravity_tasks[chat_uid] = asyncio.current_task()

    try:
        project_tree = chat_state.get("project_tree")
        file_manifest = build_server_file_manifest(chat_uid, synced_files_dict)
        pending_plan = chat_state.get("pending_plan")

        used_engine = "DIRECT"
        result = None

        # === ВЕТКА 1: Ответ пользователя на предложенный план ===
        # ВАЖНО: ветка срабатывает только на КОРОТКИЙ ответ про план.
        # Раньше сюда попадало любое сообщение, пока план висел в состоянии:
        # «Давай доделывай…» содержало «да» → повторное выполнение пустого плана
        # → в чат уходило «Готово.» на любую задачу.
        answers_plan = bool(pending_plan) and _is_plan_answer(prompt)
        if pending_plan and not answers_plan:
            logger.info("🗒️ [PLAN] Похоже на новую задачу, а не ответ на план — старый план сбрасываю")
            chat_state.pop("pending_plan", None)
            save_chat_state(chat_uid, chat_state)
            pending_plan = None

        if answers_plan:
            ans_low = prompt.strip().lower()
            if any(w in ans_low for w in ("отмен", "cancel", "не надо", "стоп")) or ans_low in ("нет", "no"):
                chat_state.pop("pending_plan", None)
                save_chat_state(chat_uid, chat_state)
                result = {
                    "text": "План отменён.",
                    "file_urls": {},
                    "environment_id": None,
                    "assigned_key": assigned_key,
                    "usage": {},
                }
            elif _is_plan_approval(ans_low):
                chat_state.pop("pending_plan", None)
                save_chat_state(chat_uid, chat_state)

                steps = pending_plan.get("steps") or []
                need_network = bool(pending_plan.get("need_network", False))
                used_engine = "ANTIGRAVITY"

                if not steps:
                    # Пустой план в состоянии (артефакт старых версий) — молча
                    # уходим на обычный разбор задачи, а не отвечаем «Готово.».
                    logger.warning("⚠️ [PLAN] pending_plan без шагов — уходим на обычный разбор задачи")
                    used_engine = "DIRECT"
                    result = None

                all_file_urls = {}
                step_reports = []
                last_env_id = None
                total_in_acc = 0
                # Бюджет на ВЕСЬ план. Раньше каждый шаг получал свежие 85K,
                # то есть план из 3 шагов мог съесть 255K при квоте 250K на аккаунт
                # и ничего не мешало агенту «думать» до потолка на каждом шаге.
                plan_budget_left = PLAN_TOKEN_BUDGET
                stopped_early = None

                for idx, st in enumerate(steps, start=1) if steps else []:
                    st_file = (st.get("file") or "").strip()
                    st_lines = (st.get("line_ranges") or "").strip()
                    st_title = (st.get("title") or st_file or f"Шаг {idx}").strip()
                    st_instr = (st.get("instruction") or "").strip()
                    st_code = (st.get("code") or "").strip()
                    if st_code:
                        # Планировщик уже написал готовый код: исполнителю не надо
                        # исследовать файл и «думать» по 1-2 минуты на виток.
                        st_instr = (
                            f"{st_instr}\n\n"
                            "ГОТОВЫЙ КОД ДЛЯ ПРАВКИ (вставь его как есть, ничего не придумывай):\n"
                            f"```\n{st_code}\n```"
                        )

                    # Шаг получает свою долю остатка, но не больше лимита вызова
                    # Antigravity (100K) и не меньше разумного минимума.
                    remaining_steps = len(steps) - idx + 1
                    step_budget = min(AG_MAX_TOTAL_TOKENS, plan_budget_left // remaining_steps)
                    if step_budget < AG_MIN_STEP_TOKENS:
                        stopped_early = (
                            f"⚠️ Остановился на шаге {idx}/{len(steps)}: на весь план "
                            f"выделено {PLAN_TOKEN_BUDGET // 1000}K токенов, остаток "
                            f"({plan_budget_left // 1000}K) уже не хватит на безопасный "
                            f"запуск песочницы."
                        )
                        logger.warning(f"🛑 [PLAN BUDGET] {stopped_early}")
                        break

                    await send_status_update({"status": f"Выполнение плана: шаг {idx}/{len(steps)} ({st_file or 'новый файл'}, бюджет {step_budget // 1000}K)..."})
                    await send_chunk_update(f"\n⚙️ **Шаг {idx}/{len(steps)}** (`{st_file}`): в работе...\n")

                    # Обновляем манифест перед каждым шагом (на случай если прошлый шаг уже изменил файл)
                    cur_manifest = build_server_file_manifest(chat_uid, {**synced_files_dict, **all_file_urls})
                    sel = [st_file] if st_file else []

                    step_res = await ai_instance.run_antigravity_agent(
                        prompt_text=st_instr,
                        status_callback=send_status_update,
                        assigned_key=assigned_key,
                        chat_uid=chat_uid,
                        recent_history=recent_history[-4:],
                        project_tree=project_tree,
                        file_manifest=cur_manifest,
                        selected_files=sel,
                        line_ranges=st_lines,
                        need_network=need_network,
                        max_total_tokens=step_budget,
                    )

                    last_env_id = step_res.get("environment_id") or last_env_id
                    assigned_key = step_res.get("assigned_key", assigned_key)
                    step_usage = step_res.get("usage") or {}
                    step_total = int(step_usage.get("total", 0) or 0) or step_budget
                    total_in_acc += int(step_usage.get("input", 0) or 0)
                    plan_budget_left = max(0, plan_budget_left - step_total)
                    logger.info(
                        f"💰 [PLAN BUDGET] шаг {idx}/{len(steps)}: истрачено {step_total} ток. | "
                        f"остаток плана {plan_budget_left}/{PLAN_TOKEN_BUDGET}"
                    )

                    step_urls = step_res.get("file_urls") or {}
                    all_file_urls.update(step_urls)

                    clean_step_txt = re.sub(r"\[SYNC:\s*[^\]]+\]", "", step_res.get("text") or "", flags=re.IGNORECASE).strip()
                    step_reports.append(f"✅ **Шаг {idx}/{len(steps)} ({st_title}):** {clean_step_txt or 'готово'}")

                if stopped_early:
                    step_reports.append(stopped_early)

                if steps:
                    sync_block = "\n".join(f"[SYNC: {p}]" for p in all_file_urls.keys())
                    final_txt = "\n\n".join(step_reports)
                    if sync_block:
                        final_txt = f"{sync_block}\n\n{final_txt}"

                    result = {
                        "text": final_txt.strip(),
                        "file_urls": all_file_urls,
                        "environment_id": last_env_id,
                        "assigned_key": assigned_key,
                        "usage": {"input": total_in_acc},
                    }
            else:
                # Пользователь написал правку к плану — сбрасываем старый план и идём дальше как новый запрос
                chat_state.pop("pending_plan", None)
                save_chat_state(chat_uid, chat_state)

        # === ВЕТКА 2: Ответ IDE на тег, который запросил сам Antigravity ===
        # GET_FILES/GET_TREE — это теги уровня DIRECT (диспетчера), поэтому они
        # обрабатываются в ветке 3. Раньше при last_engine == "ANTIGRAVITY" ответ IDE
        # с файлами уходил прямо в песочницу, и модель отвечала «сходи сам curl'ом».
        if result is None and is_tool_response and last_engine == "ANTIGRAVITY" and last_tag not in ("GET_FILES", "GET_TREE"):
            used_engine = "ANTIGRAVITY"
            result = await ai_instance.run_antigravity_agent(
                prompt_text=prompt,
                status_callback=send_status_update,
                assigned_key=assigned_key,
                chat_uid=chat_uid,
                recent_history=recent_history,
                project_tree=project_tree,
                file_manifest=file_manifest,
                selected_files=list(target_files) if target_files else [],
                need_network=False,
                max_total_tokens=AG_MAX_TOTAL_TOKENS,
            )

        # === ВЕТКА 2.5: IDE принесла файлы по [GET_FILES] от Планировщика ===
        # Пользователь не должен писать «Запроси эти файлы» руками: файлы уже на
        # сервере, поэтому один раз молча повторяем исходную задачу — DIRECT
        # увидит их в манифесте и сразу уйдёт в [CALL_PLANNER].
        auto_retry_task = chat_state.pop("planner_waiting_files", None)
        if result is None and is_tool_response and auto_retry_task:
            logger.info(f"🔁 [AUTO-RETRY] Файлы от IDE получены → повторяем задачу планировщика: {auto_retry_task[:80]}")
            prompt = auto_retry_task
            is_tool_response = False
            # Файлы доехали — счётчик неудачных запросов сбрасываем, иначе он
            # копился бы по всему чату и однажды заблокировал бы файл, который
            # в другой задаче просто ещё не успели прислать.
            if chat_state.pop("need_files_asked", None) is not None:
                save_chat_state(chat_uid, chat_state)

        # === ВЕТКА 3: Обычный вход через Direct (3.5-flash-lite) -> при работе с файлами в Планировщик ===
        if result is None:
            direct_res = await ai_instance.run_direct(
                prompt_text=prompt,
                recent_history=recent_history,
                project_tree=project_tree,
                file_manifest=file_manifest,
                status_callback=send_status_update,
                # Стрим не отдаём в UI: если это [RUN_COMMAND]/[CALL_…]/патч,
                # пользователь не должен видеть «Действие: cmd.exe …» в бабле.
                # Обычный текстовый ответ всё равно приходит в antigravity_result.
                chunk_callback=None,
                assigned_key=assigned_key,
            )

            # Перехват «сходи скачай curl'ом то, что уже есть на сервере».
            # DIRECT присылает такую команду как обычный текст (escalate=False),
            # поэтому проверять её нужно ДО развилки, иначе она уходит в IDE
            # как cmd.exe-команда и ничего не меняет.
            curl_urls = detect_run_command_urls(direct_res.get("text") or "")
            guard_used = int(chat_state.get("curl_guard_hits", 0) or 0)
            stale_curl = [u for u in curl_urls if _manifest_has_url(file_manifest, u)]
            if stale_curl and guard_used < CURL_GUARD_LIMIT:
                chat_state["curl_guard_hits"] = guard_used + 1
                save_chat_state(chat_uid, chat_state)
                guarded_files = direct_res.get("ag_files") or [
                    f["path"] for f in file_manifest if _manifest_has_url([f], stale_curl[0])
                ]
                logger.warning(
                    f"🛑 [CURL GUARD] DIRECT собирался скачивать curl'ом файлы, которые уже на сервере "
                    f"({len(stale_curl)} URL) → эскалация в планировщик: {guarded_files}"
                )
                direct_res = {
                    "escalate": True,
                    "ag_task": prompt,
                    "ag_files": guarded_files,
                    "need_network": False,
                    "assigned_key": direct_res.get("assigned_key", assigned_key),
                }

            if not direct_res.get("escalate"):
                used_engine = "DIRECT"
                direct_edits = direct_res.get("direct_edits") or {}
                applied_urls = _apply_direct_edits(chat_uid, direct_edits) if direct_edits else {}
                direct_text = direct_res["text"]
                if applied_urls:
                    logger.info(f"🩹 [DIRECT EDIT] Изменено на сервере: {', '.join(applied_urls)}")
                    direct_text = _inject_sync_tag(direct_text, list(applied_urls))
                result = {
                    "text": direct_text,
                    "file_urls": applied_urls,
                    "environment_id": None,
                    "assigned_key": direct_res.get("assigned_key", assigned_key),
                    "usage": direct_res.get("usage", {}),
                }
            else:
                cand_files = direct_res.get("ag_files") or []
                manifest_paths = {f["path"] for f in file_manifest}
                manifest_names = {os.path.basename(p) for p in manifest_paths}

                def _missing(cands):
                    return [
                        f for f in cands
                        if f not in manifest_paths
                        and os.path.basename(f) not in manifest_names
                        and project_tree
                        and os.path.basename(f) in project_tree
                    ]

                missing_on_server = _missing(cand_files)

                if missing_on_server:
                    logger.info(f"📥 [PRE-FETCH] Файлы не на сервере, запрашиваем через IDE: {missing_on_server}")
                    used_engine = "DIRECT"
                    pre_tag, pre_blocked = partition_get_files(missing_on_server)
                    result = {
                        "text": pre_tag or _comma_name_error(pre_blocked),
                        "file_urls": {},
                        "environment_id": None,
                        "assigned_key": direct_res.get("assigned_key", assigned_key),
                        "usage": {},
                    }
                else:
                    # Всегда идём через Планировщик (Hydrogen, 250K TPM)
                    used_engine = "PLANNER"
                    planner_task = direct_res.get("ag_task") or prompt
                    need_network = bool(direct_res.get("need_network", False))

                    try:
                        plan_res = await ai_instance.run_planner(
                            task_prompt=planner_task,
                            recent_history=recent_history,
                            project_tree=project_tree,
                            file_manifest=file_manifest,
                            selected_files=cand_files,
                            status_callback=send_status_update,
                            assigned_key=assigned_key,
                        )
                    except EmptyPlanError as pe:
                        logger.warning(f"⚠️ [PLANNER EMPTY PLAN] {pe}")
                        err_text = summarize_planner_failure(
                            getattr(pe, "raw_text", ""),
                            getattr(pe, "out_tokens", 0),
                            getattr(pe, "reason", ""),
                            getattr(pe, "asked_files", None),
                        )
                        plan_res = {
                            "plan": {
                                "need_files": [], "steps": [], "summary": "",
                                "_error": "empty_plan",
                                "_error_text": err_text,
                            },
                            "assigned_key": assigned_key,
                            "usage": {},
                        }
                    plan = plan_res["plan"]
                    assigned_key = plan_res.get("assigned_key", assigned_key)

                    def _norm_need(raw):
                        return re.sub(r"^(\./)+", "", str(raw).strip().replace("\\", "/")).lstrip("/")

                    need_files = [_norm_need(nf) for nf in (plan.get("need_files") or []) if str(nf).strip()]
                    need_files = [nf for nf in need_files if nf not in manifest_paths]

                    # Планировщик иногда вместо "need_files" пишет в summary
                    # «необходимо проверить js/entities.js, js/interiors.js…».
                    # Пользователю это читать незачем: сами запрашиваем файлы у IDE.
                    mentioned = _summary_need_files(plan, project_tree, manifest_paths)
                    if mentioned:
                        logger.info(
                            f"📥 [PLANNER MENTIONS] Файлы названы в summary, а не в need_files: {mentioned}"
                        )
                        need_files = mentioned
                        plan = dict(plan)
                        plan["need_files"] = mentioned
                        plan["summary"] = ""

                    if need_files:
                        get_tag, comma_files = partition_get_files(need_files)

                        # Имя с запятой протокол не переживает: IDE увидит два
                        # несуществующих файла и будет получать «не найден»
                        # вечно. Честно говорим об этом сразу.
                        if comma_files:
                            logger.warning(
                                f"🛑 [GET_FILES] Имя с запятой — протокол не поддерживает: {comma_files}"
                            )
                            result = {
                                "text": _comma_name_error(comma_files),
                                "file_urls": {},
                                "environment_id": None,
                                "assigned_key": assigned_key,
                                "usage": plan_res.get("usage", {}),
                            }
                        else:
                            # Сколько раз уже просили КАЖДЫЙ файл. Если просили
                            # дважды, а IDE так и не прислала — третий запрос
                            # бессмысленен, это и был бесконечный цикл.
                            asked = chat_state.get("need_files_asked") or {}
                            hopeless = [nf for nf in need_files if int(asked.get(nf, 0)) >= 2]
                            if hopeless:
                                logger.warning(
                                    "🛑 [GET_FILES LOOP] Файл(ы) просили дважды, IDE не прислала: "
                                    f"{', '.join(hopeless)} — прекращаю цикл"
                                )
                                result = {
                                    "text": (
                                        "⚠️ Не смог получить файл(ы) с ПК: **"
                                        + "**, **".join(hopeless)
                                        + "**\n\nIDE дважды ответила, что их нет в проекте. "
                                        "Проверь имя файла в дереве проекта и напиши точное "
                                        "название — возможно, есть опечатка."
                                    ),
                                    "file_urls": {},
                                    "environment_id": None,
                                    "assigned_key": assigned_key,
                                    "usage": plan_res.get("usage", {}),
                                }
                            else:
                                for nf in need_files:
                                    asked[nf] = int(asked.get(nf, 0)) + 1
                                chat_state["need_files_asked"] = asked
                                chat_state["planner_waiting_files"] = planner_task
                                save_chat_state(chat_uid, chat_state)
                                logger.info(
                                    f"📥 [PLANNER NEED FILES] Дозапрашиваем файлы: {need_files} "
                                    f"(попытки: {asked})"
                                )
                                result = {
                                    "text": get_tag,
                                    "file_urls": {},
                                    "environment_id": None,
                                    "assigned_key": assigned_key,
                                    "usage": plan_res.get("usage", {}),
                                }
                    elif not (plan.get("steps") or []):
                        # План без шагов: либо реальный ответ по файлам (summary непустой),
                        # либо модель не справилась. Раньше во втором случае в чат уходило
                        # «Готово.» — именно это и выглядело как «на любое сообщение Готово».
                        answer = (plan.get("summary") or "").strip()
                        if answer:
                            result = {
                                "text": answer,
                                "file_urls": {},
                                "environment_id": None,
                                "assigned_key": assigned_key,
                                "usage": plan_res.get("usage", {}),
                            }
                        else:
                            logger.warning(
                                "⚠️ [PLANNER EMPTY PLAN] Планировщик вернул пустой план — "
                                "отдаём понятную ошибку вместо «Готово.»"
                            )
                            result = {
                                "text": (
                                    plan.get("_error_text")
                                    or (
                                        "⚠️ Не смог разобрать задачу: планировщик вернул пустой план "
                                        "(без шагов и без запроса файлов).\n\n"
                                        "Попробуй переформулировать: укажи конкретный файл и что в нём "
                                        "поменять."
                                    )
                                ),
                                "file_urls": {},
                                "environment_id": None,
                                "assigned_key": assigned_key,
                                "usage": plan_res.get("usage", {}),
                            }
                    else:
                        # Сохраняем план и выводим его пользователю в чат с кнопками подтверждения
                        chat_state["pending_plan"] = {
                            "summary": plan.get("summary", ""),
                            "steps": plan.get("steps", []),
                            "need_network": need_network,
                        }
                        save_chat_state(chat_uid, chat_state)
                        plan_text = _format_plan_message(plan)
                        result = {
                            "text": plan_text,
                            "file_urls": {},
                            "environment_id": None,
                            "assigned_key": assigned_key,
                            "usage": plan_res.get("usage", {}),
                        }

        # Страховка: DIRECT не имеет терминала. Даже если модель всё-таки выдала
        # команду — вырезаем её из финального текста (иначе IDE выполнит cmd.exe).
        agent_text_raw = result["text"]
        agent_text_clean, stripped_cmds = _strip_run_command(agent_text_raw)
        if stripped_cmds:
            logger.warning(
                f"🚫 [NO TERMINAL] В итоговом ответе вырезано команд: {len(stripped_cmds)} "
                f"({str(stripped_cmds[0])[:100]})"
            )
            result["text"] = agent_text_clean

        new_env_id = result.get("environment_id")
        agent_reply_text = result["text"]
        final_assigned_key = result.get("assigned_key", assigned_key)
        last_in_tokens = int((result.get("usage") or {}).get("input", 0) or 0)

        next_tag = detect_requested_ide_tag(agent_reply_text)
        if next_tag:
            chat_state["last_requested_tag"] = next_tag
            save_chat_state(chat_uid, chat_state)

        logger.info(f"📤 [DONE] ({used_engine}) Ответ готов ({len(agent_reply_text)} симв.): '{agent_reply_text[:60]}...'")

        conn = None; cursor = None
        try:
            conn = await get_async_db_connection()
            cursor = await conn.cursor()

            new_files = result.get("file_urls", {})
            synced_str = None
            if new_files:
                merged_files = dict(synced_files_dict)
                merged_files.update(new_files)
                synced_str = json.dumps(merged_files, ensure_ascii=False)

            if need_title_update:
                await cursor.execute(
                    """UPDATE antigravity_chats 
                       SET last_interaction_id = NULL, environment_id = COALESCE(%s, environment_id), key_index = %s, title = %s, 
                           synced_files = COALESCE(%s, synced_files), last_engine = %s, last_input_tokens = %s, updated_at = NOW() 
                       WHERE chat_uid = %s""",
                    (new_env_id, final_assigned_key, auto_title, synced_str, used_engine, last_in_tokens, chat_uid)
                )
            else:
                await cursor.execute(
                    """UPDATE antigravity_chats 
                       SET last_interaction_id = NULL, environment_id = COALESCE(%s, environment_id), key_index = %s, 
                           synced_files = COALESCE(%s, synced_files), last_engine = %s, last_input_tokens = %s, updated_at = NOW() 
                       WHERE chat_uid = %s""",
                    (new_env_id, final_assigned_key, synced_str, used_engine, last_in_tokens, chat_uid)
                )

            await cursor.execute(
                "INSERT INTO antigravity_messages (chat_uid, role, text) VALUES (%s, 'agent', %s)",
                (chat_uid, agent_reply_text)
            )
            await conn.commit()
        except Exception as dbe2:
            logger.error(f"[DB Error After Task] {dbe2}")
        finally:
            if cursor: await cursor.close()
            if conn: conn.close()

        await send_antigravity_event(user_token, websocket, {
            "type": "antigravity_result",
            "ui_msg_id": ui_msg_id,
            "text": agent_reply_text,
            "file_urls": result.get("file_urls", {}),
            "file_meta": build_synced_meta(chat_uid, result.get("file_urls", {})),
            "environment_id": new_env_id,
            "interaction_id": None,
            "chat_id": chat_uid
        })

    except asyncio.CancelledError:
        logger.warning(f"🛑 [WS Antigravity] Задача {chat_uid} прервана! Очистка...")
        conn = None; cursor = None
        try:
            conn = await get_async_db_connection()
            cursor = await conn.cursor()
            await cursor.execute(
                "DELETE FROM antigravity_messages WHERE chat_uid = %s AND role IN ('user', 'tool') ORDER BY id DESC LIMIT 1",
                (chat_uid,)
            )
            await cursor.execute(
                "UPDATE antigravity_chats SET last_interaction_id = NULL, environment_id = NULL WHERE chat_uid = %s",
                (chat_uid,)
            )
            await conn.commit()
        except Exception as cle:
            logger.warning(f"[WS Cleanup Error] {cle}")
        finally:
            if cursor: await cursor.close()
            if conn: conn.close()

        await send_antigravity_event(user_token, websocket, {
            "type": "antigravity_cancelled",
            "chat_id": chat_uid,
            "ui_msg_id": ui_msg_id
        })
    except Exception as e:
        logger.error(f"[WS Antigravity Error] {e}", exc_info=True)
        await send_antigravity_event(user_token, websocket, {
            "type": "antigravity_error",
            "ui_msg_id": ui_msg_id,
            "error": str(e)
        })
    finally:
        active_antigravity_tasks.pop(chat_uid, None)