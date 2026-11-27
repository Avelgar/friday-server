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
from app.services.ai.router import _inject_sync_tag
from app.websocket_server.utils import async_send
from app.websocket_server.antigravity_storage import (
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
        if pending_plan:
            ans_low = prompt.strip().lower()
            if any(w in ans_low for w in ("отмен", "cancel", "не надо", "стоп")) or ans_low == "нет":
                chat_state.pop("pending_plan", None)
                save_chat_state(chat_uid, chat_state)
                result = {
                    "text": "План отменён.",
                    "file_urls": {},
                    "environment_id": None,
                    "assigned_key": assigned_key,
                    "usage": {},
                }
            elif any(w in ans_low for w in ("выполн", "запуст", "поехали", "да", "ok", "ок", "старт")):
                chat_state.pop("pending_plan", None)
                save_chat_state(chat_uid, chat_state)

                steps = pending_plan.get("steps") or []
                need_network = bool(pending_plan.get("need_network", False))
                used_engine = "ANTIGRAVITY"

                all_file_urls = {}
                step_reports = []
                last_env_id = None
                total_in_acc = 0

                for idx, st in enumerate(steps, start=1):
                    st_file = (st.get("file") or "").strip()
                    st_lines = (st.get("line_ranges") or "").strip()
                    st_title = (st.get("title") or st_file or f"Шаг {idx}").strip()
                    st_instr = (st.get("instruction") or "").strip()

                    await send_status_update({"status": f"Выполнение плана: шаг {idx}/{len(steps)} ({st_file or 'новый файл'})..."})
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
                        max_total_tokens=85_000,
                    )

                    last_env_id = step_res.get("environment_id") or last_env_id
                    assigned_key = step_res.get("assigned_key", assigned_key)
                    total_in_acc += int((step_res.get("usage") or {}).get("input", 0) or 0)

                    step_urls = step_res.get("file_urls") or {}
                    all_file_urls.update(step_urls)

                    clean_step_txt = re.sub(r"\[SYNC:\s*[^\]]+\]", "", step_res.get("text") or "", flags=re.IGNORECASE).strip()
                    step_reports.append(f"✅ **Шаг {idx}/{len(steps)} ({st_title}):** {clean_step_txt or 'готово'}")

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
                max_total_tokens=85_000,
            )

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
                    result = {
                        "text": f"[GET_FILES: {', '.join(missing_on_server)}]",
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

                    plan_res = await ai_instance.run_planner(
                        task_prompt=planner_task,
                        recent_history=recent_history,
                        project_tree=project_tree,
                        file_manifest=file_manifest,
                        selected_files=cand_files,
                        status_callback=send_status_update,
                        assigned_key=assigned_key,
                    )
                    plan = plan_res["plan"]
                    assigned_key = plan_res.get("assigned_key", assigned_key)

                    need_files = [
                        re.sub(r"^(\./)+", "", str(nf).strip().replace("\\", "/")).lstrip("/")
                        for nf in (plan.get("need_files") or [])
                        if str(nf).strip()
                    ]
                    need_files = [nf for nf in need_files if nf not in manifest_paths]

                    if need_files:
                        logger.info(f"📥 [PLANNER NEED FILES] Дозапрашиваем файлы: {need_files}")
                        result = {
                            "text": f"[GET_FILES: {', '.join(need_files)}]",
                            "file_urls": {},
                            "environment_id": None,
                            "assigned_key": assigned_key,
                            "usage": plan_res.get("usage", {}),
                        }
                    elif not (plan.get("steps") or []):
                        # Чистый анализ/ответ без правки файлов — Планировщик уже ответил сам!
                        result = {
                            "text": (plan.get("summary") or "Готово.").strip(),
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

        # Страховка: DIRECT иногда всё же возвращает «Действие: cmd.exe … curl …».
        # Команда ничего не меняет на сервере и ломает виджет «Действия» в IDE,
        # поэтому такой ответ превращаем в обычную текстовую подсказку.
        agent_text_raw = result["text"]
        if detect_run_command_urls(agent_text_raw):
            logger.warning("🛑 [CURL GUARD] Подавляем RUN_COMMAND с curl в итоговом ответе")
            result["text"] = (
                "⚠️ Файлы уже лежат на сервере, скачивать их командой не нужно. "
                "Напиши, что именно поправить — я запущу правку в песочнице."
            )

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