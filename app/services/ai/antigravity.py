# -*- coding: utf-8 -*-
import os
import re
import time
import asyncio
import logging
import threading
import urllib.parse
from collections import deque
from .key_pool import GeminiKeyPool, create_genai_client, extract_interaction_text, extract_usage_tokens
from .prompts import AGENT_SYSTEM_INSTRUCTION
from .router import format_stateless_prompt_parts
from .model_config import get_models_config, normalize_model_name

logger = logging.getLogger("AI_Service")
SYS_DIRS = {".", "..", "usr", "bin", "lib", "lib64", "lib32", "libx32", "sbin", "proc", "sys", "dev", "etc", "var", "boot", "run", "srv", "snap", "media", "mnt"}


class BudgetExhaustedError(RuntimeError):
    """Остановка по потолку max_total_tokens."""


class AsyncAntigravityLimiter:
    """Асинхронное скользящее окно RPM и TPM (60 сек) с учётом total (включая cached)."""

    def __init__(self, max_rpm: int = 50, max_tpm: int = 92_000, min_headroom: int = 45_000):
        self.max_rpm = max_rpm
        self.max_tpm = max_tpm
        self.min_headroom = min_headroom
        self._lock = asyncio.Lock()
        self._entries: deque[list] = deque()

    def _prune(self, now: float) -> None:
        cutoff = now - 60.0
        while self._entries and self._entries[0][0] < cutoff:
            self._entries.popleft()

    async def acquire(self, estimated_tokens: int, status_callback=None) -> list:
        need = max(self.min_headroom, estimated_tokens)
        while True:
            sleep_for = 0.0
            used_tpm = 0
            async with self._lock:
                now = time.monotonic()
                self._prune(now)
                used_rpm = len(self._entries)
                used_tpm = sum(item[1] for item in self._entries)

                if used_rpm < self.max_rpm and (used_tpm + need <= self.max_tpm or used_rpm == 0):
                    slot = [now, estimated_tokens]
                    self._entries.append(slot)
                    return slot

                if self._entries:
                    sleep_for = max(1.0, (self._entries[0][0] + 60.5) - now)
                else:
                    sleep_for = 1.0

            logger.info(f"⏳ [AG LIMITER] Окно TPM заполнено ({used_tpm}/{self.max_tpm}), ждём {sleep_for:.1f}с...")
            if status_callback:
                await status_callback({"status": f"Ожидание сброса окна TPM ({sleep_for:.0f}с)..."})
            await asyncio.sleep(sleep_for)

    async def finalize(self, slot: list, actual_total_tokens: int) -> None:
        if actual_total_tokens <= 0:
            return
        async with self._lock:
            slot[0] = time.monotonic()
            slot[1] = actual_total_tokens


ag_limiter = AsyncAntigravityLimiter(max_rpm=50, max_tpm=92_000, min_headroom=45_000)


def _approx_tokens(text: str) -> int:
    return max(0, (len(text or "") + 2) // 3)


def _describe_step(step) -> str:
    stype = getattr(step, "type", None) or getattr(step, "step_type", None) or type(step).__name__
    content = getattr(step, "content", None) or []
    preview = ""
    if isinstance(content, list) and content:
        first = content[0]
        preview = (
            getattr(first, "text", None)
            or getattr(first, "name", None)
            or getattr(first, "command", None)
            or ""
        )
    preview = str(preview).replace("\n", " ").strip()[:80]
    return f"{stype}: {preview}" if preview else str(stype)


def _sanitize_sync_path(raw_path: str) -> str | None:
    p = raw_path.strip().replace("\\", "/")
    p = re.sub(r"^(\./)+", "", p).lstrip("/")
    if not p or p in (".", "..", "...") or set(p) <= {".", "/"}:
        return None
    if ".." in p.split("/"):
        return None
    return p


def _extract_and_normalize_sync_tags(output_text: str) -> tuple[str, list[str]]:
    text = output_text or ""
    found: list[str] = []

    def _norm_bracket(m):
        clean = _sanitize_sync_path(m.group(1))
        if clean:
            if clean not in found:
                found.append(clean)
            return f"[SYNC: {clean}]"
        return m.group(0)

    text = re.sub(r"\[SYNC:\s*([^\]]+)\]", _norm_bracket, text, flags=re.IGNORECASE)

    if not found:
        def _norm_bare(m):
            clean = _sanitize_sync_path(m.group(1))
            if clean and ("." in os.path.basename(clean) or "/" in clean):
                if clean not in found:
                    found.append(clean)
                return f"[SYNC: {clean}]"
            return m.group(0)

        text = re.sub(r"(?im)^\s*sync:\s*([^\s,\]]+)\s*$", _norm_bare, text)

    return text, found


def _extract_text_any(interaction, streamed_fallback: str = "") -> str:
    txt = extract_interaction_text(interaction)
    if txt and txt.strip():
        return txt
    outputs = getattr(interaction, "outputs", None) or []
    parts = []
    for out in outputs:
        t = getattr(out, "text", None)
        if not t and isinstance(out, dict):
            t = out.get("text")
        if t:
            parts.append(t)
    if parts:
        return "\n".join(parts)
    if streamed_fallback and streamed_fallback.strip():
        return streamed_fallback.strip()
    return ""


async def _cancel_interaction_safe(api_keys: list[str], key_index: int, interaction_id: str):
    try:
        c = create_genai_client(api_keys, key_index, timeout=10000, with_retry_opt=False)
        await asyncio.wait_for(asyncio.to_thread(c.interactions.cancel, id=interaction_id), timeout=10.0)
        logger.info(f"🛑 [GOOGLE CANCELLED] Отменили {interaction_id}")
    except Exception:
        pass


async def _observe_interaction(
    api_keys: list[str],
    assigned_key: int,
    interaction_id: str,
    ag_model: str,
    status_callback=None,
    timeout_sec: float = 300.0,
) -> tuple[object, str]:
    loop = asyncio.get_running_loop()
    start_ts = time.monotonic()
    deadline = start_ts + timeout_sec
    last_event_id = None
    live_step_num = 0
    current_step_type = ""
    streamed_text_parts: list[str] = []
    consecutive_errors = 0
    use_streaming = True

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Песочница ({ag_model}) не завершилась за {int(timeout_sec)} секунд.")

        if not use_streaming:
            poll_client = create_genai_client(api_keys, assigned_key, timeout=60000)
            logged_steps = 0
            poll_count = 0
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Песочница ({ag_model}) зависла дольше {int(timeout_sec)} секунд.")
                await asyncio.sleep(2.5)
                poll_count += 1
                try:
                    interaction = await asyncio.wait_for(
                        asyncio.to_thread(poll_client.interactions.get, id=interaction_id),
                        timeout=25.0,
                    )
                    consecutive_errors = 0
                except asyncio.CancelledError:
                    raise
                except Exception as pe:
                    consecutive_errors += 1
                    logger.warning(f"⚠️ [POLL RETRY {consecutive_errors}/8] Ключ #{assigned_key}: {pe}")
                    if consecutive_errors >= 8:
                        raise
                    await asyncio.sleep(2.0)
                    continue

                steps = getattr(interaction, "steps", None) or []
                while logged_steps < len(steps):
                    logger.info(f"🧩 [ANTIGRAVITY STEP #{logged_steps + 1}] {_describe_step(steps[logged_steps])}")
                    logged_steps += 1

                status_raw = str(getattr(interaction, "status", "")).lower().strip()
                elapsed = time.monotonic() - start_ts
                if poll_count % 6 == 0:
                    logger.info(f"⏳ [ANTIGRAVITY POLLING] Статус: {status_raw} ({elapsed:.0f}с)")
                if status_callback:
                    await status_callback({"status": f"Агент работает ({elapsed:.0f}с)..."})
                if status_raw in ("completed", "incomplete", "failed", "cancelled") or any(k in status_raw for k in ("complete", "fail", "cancel")):
                    return interaction, "".join(streamed_text_parts)

        event_queue: asyncio.Queue = asyncio.Queue()
        stop_event = threading.Event()
        stream_holder = {"stream": None}

        def _stream_worker():
            try:
                sc = create_genai_client(api_keys, assigned_key, timeout=120000, with_retry_opt=False)
                get_kwargs = {"id": interaction_id, "stream": True}
                if last_event_id:
                    get_kwargs["last_event_id"] = last_event_id
                s = sc.interactions.get(**get_kwargs)
                stream_holder["stream"] = s
                for ev in s:
                    if stop_event.is_set():
                        break
                    try:
                        loop.call_soon_threadsafe(event_queue.put_nowait, ("event", ev))
                    except RuntimeError:
                        break
                try:
                    loop.call_soon_threadsafe(event_queue.put_nowait, ("eof", None))
                except RuntimeError:
                    pass
            except Exception as ex:
                try:
                    loop.call_soon_threadsafe(event_queue.put_nowait, ("err", ex))
                except RuntimeError:
                    pass
            finally:
                try:
                    if stream_holder["stream"] and hasattr(stream_holder["stream"], "close"):
                        stream_holder["stream"].close()
                except Exception:
                    pass

        worker_task = asyncio.create_task(asyncio.to_thread(_stream_worker))
        event_interaction = None
        got_terminal_event = False

        try:
            while True:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    raise TimeoutError(f"Песочница ({ag_model}) не завершилась за {int(timeout_sec)} секунд.")
                try:
                    kind, payload = await asyncio.wait_for(event_queue.get(), timeout=min(10.0, rem))
                except asyncio.TimeoutError:
                    elapsed = time.monotonic() - start_ts
                    if status_callback:
                        await status_callback({"status": f"Агент работает ({elapsed:.0f}с, шагов: {live_step_num})..."})
                    continue

                if kind == "event":
                    consecutive_errors = 0
                    ev = payload
                    eid = getattr(ev, "event_id", None)
                    if eid:
                        last_event_id = eid

                    etype = str(getattr(ev, "event_type", "") or "").lower()
                    elapsed = time.monotonic() - start_ts

                    if etype == "step.start":
                        live_step_num += 1
                        step_obj = getattr(ev, "step", None)
                        current_step_type = str(getattr(step_obj, "type", None) or "step").lower()
                        logger.info(f"🧩 [AG LIVE #{live_step_num}] Старт: {current_step_type} ({elapsed:.0f}с)")
                        if status_callback:
                            await status_callback({"status": f"Шаг #{live_step_num}: {current_step_type} ({elapsed:.0f}с)..."})

                    elif etype == "step.delta":
                        delta = getattr(ev, "delta", None)
                        if delta is not None:
                            dtype = str(getattr(delta, "type", "") or "").lower()
                            dtext = getattr(delta, "text", None)
                            if dtext and dtype != "thought_summary" and (dtype == "text" or current_step_type == "model_output"):
                                streamed_text_parts.append(str(dtext))

                    elif etype == "step.stop":
                        u = getattr(ev, "usage", None) or getattr(ev, "step_usage", None)
                        if u:
                            inp = getattr(u, "total_input_tokens", None) or 0
                            cac = getattr(u, "total_cached_tokens", None) or 0
                            out = getattr(u, "total_output_tokens", None) or 0
                            tot = getattr(u, "total_tokens", None) or 0
                            logger.info(f"   ↳ [STEP #{live_step_num} TOKENS] in={inp} cached={cac} out={out} total={tot}")

                    elif etype in ("interaction.completed", "interaction.incomplete", "interaction.failed", "interaction.cancelled"):
                        event_interaction = getattr(ev, "interaction", None)
                        got_terminal_event = True
                        logger.info(f"🏁 [ANTIGRAVITY STREAM DONE] {etype} ({elapsed:.0f}с)")
                        break

                elif kind == "eof":
                    break

                elif kind == "err":
                    err_obj = payload
                    err_low = str(err_obj).lower()
                    if any(w in err_low for w in ("stream", "unexpected keyword", "invalid_argument", "not implemented")):
                        logger.warning(f"⚠️ [STREAM UNSUPPORTED] Переключаемся на polling: {err_obj}")
                        use_streaming = False
                        break
                    consecutive_errors += 1
                    logger.warning(f"⚠️ [STREAM RECONNECT {consecutive_errors}/6] {err_obj}")
                    if consecutive_errors >= 6:
                        use_streaming = False
                    await asyncio.sleep(2.0)
                    break

        finally:
            stop_event.set()
            try:
                if stream_holder["stream"] and hasattr(stream_holder["stream"], "close"):
                    stream_holder["stream"].close()
            except Exception:
                pass
            if not worker_task.done():
                worker_task.cancel()
                try:
                    await worker_task
                except BaseException:
                    pass

        checked = None
        try:
            check_client = create_genai_client(api_keys, assigned_key, timeout=30000)
            checked = await asyncio.wait_for(asyncio.to_thread(check_client.interactions.get, id=interaction_id), timeout=25.0)
        except Exception as ge:
            logger.warning(f"⚠️ [GET FINAL INTERACTION ERR] {ge}")

        final_obj = checked or event_interaction
        if final_obj is not None:
            st_raw = str(getattr(final_obj, "status", "")).lower().strip()
            if got_terminal_event or st_raw in ("completed", "incomplete", "failed", "cancelled") or any(k in st_raw for k in ("complete", "fail", "cancel")):
                return final_obj, "".join(streamed_text_parts)

        await asyncio.sleep(1.5)


async def run_antigravity_agent(
    key_pool: GeminiKeyPool,
    api_keys: list[str],
    prompt_text: str,
    status_callback=None,
    assigned_key: int = None,
    chat_uid: str = None,
    recent_history: list = None,
    project_tree: str = None,
    file_manifest: list = None,
    selected_files: list = None,
    line_ranges: str = "",
    need_network: bool = False,
    max_total_tokens: int = 85_000,
    per_request_key: bool = True,
    **kwargs,
):
    model_tag = "antigravity"
    # Каждый новый запуск песочницы — на самом свободном ключе пула.
    if per_request_key:
        assigned_key = key_pool.pick_key_for_request(model=model_tag, avoid_index=assigned_key)
    elif assigned_key is None or not key_pool.is_usable(assigned_key, model=model_tag):
        assigned_key = key_pool.get_best_key(model=model_tag)

    ag_model = normalize_model_name(get_models_config()["antigravity_model"])

    effective_prompt = prompt_text
    if line_ranges and line_ranges.strip():
        effective_prompt = f"{prompt_text}\n\nСТРОКИ ДЛЯ ЧТЕНИЯ (используй sed -n, не выводи весь файл): {line_ranges.strip()}"

    parts = format_stateless_prompt_parts(
        prompt_text=effective_prompt,
        recent_history=recent_history,
        project_tree=project_tree,
        file_manifest=file_manifest,
        selected_files=selected_files,
    )
    full_input = parts["full_text"]

    sys_tok = _approx_tokens(AGENT_SYSTEM_INSTRUCTION)
    tree_tok = _approx_tokens(parts["tree"])
    man_tok = _approx_tokens(parts["manifest"])
    hist_tok = _approx_tokens(parts["history"])
    task_tok = _approx_tokens(parts["task"])
    est_tokens = max(500, sys_tok + tree_tok + man_tok + hist_tok + task_tok)

    logger.info(
        "📤 [AG PAYLOAD BREAKDOWN]\n"
        f"   модель={ag_model} | max_total_tokens={max_total_tokens} | key=#{assigned_key} | network={need_network}\n"
        f"   ├─ system_instruction: {len(AGENT_SYSTEM_INSTRUCTION)} симв. (~{sys_tok} ток.)\n"
        f"   ├─ дерево проекта:     {len(parts['tree'])} симв. (~{tree_tok} ток.)\n"
        f"   ├─ список curl:        {len(parts['manifest'])} симв. (~{man_tok} ток.)\n"
        f"   ├─ история:            {len(parts['history'])} симв. (~{hist_tok} ток.)\n"
        f"   ├─ запрос:             {len(parts['task'])} симв. (~{task_tok} ток.)\n"
        f"   └─ ОЦЕНКА СТАРТА (in): ~{est_tokens} ток."
    )
    logger.info(f"=== [AG SYSTEM INSTRUCTION] ===\n{AGENT_SYSTEM_INSTRUCTION}")
    logger.info(f"=== [AG INPUT] ===\n{full_input}")

    tools = [{"type": "code_execution"}]
    if need_network:
        tools.extend([{"type": "google_search"}, {"type": "url_context"}])

    slot = await ag_limiter.acquire(est_tokens, status_callback=status_callback)
    new_interaction_id = None

    try:
        if status_callback:
            await status_callback({"status": f"Запуск песочницы ({ag_model}, ключ #{assigned_key})..."})

        req_kwargs = {
            "agent": "antigravity-preview-09-2026",
            "background": True,
            "system_instruction": AGENT_SYSTEM_INSTRUCTION,
            "tools": tools,
            "agent_config": {
                "type": "antigravity",
                "model": ag_model,
                "max_total_tokens": str(max_total_tokens),
            },
            "environment": {
                "type": "remote",
            },
            "input": full_input,
        }

        create_client = create_genai_client(api_keys, assigned_key, timeout=25000)
        interaction = await asyncio.wait_for(
            asyncio.to_thread(create_client.interactions.create, **req_kwargs),
            timeout=30.0
        )

        new_interaction_id = interaction.id
        env_id = interaction.environment_id
        logger.info(f"🆔 [GOOGLE CREATED] interaction_id={new_interaction_id} | env_id={env_id}")

        if status_callback:
            await status_callback({"status": f"Агент запущен ({ag_model})...", "interaction_id": new_interaction_id})

        interaction, streamed_text = await _observe_interaction(
            api_keys=api_keys,
            assigned_key=assigned_key,
            interaction_id=new_interaction_id,
            ag_model=ag_model,
            status_callback=status_callback,
            timeout_sec=300.0,
        )

        status_raw = str(getattr(interaction, "status", "")).lower().strip()
        usage = extract_usage_tokens(interaction)
        new_in = max(0, usage["input"] - usage["cached"])
        total_for_limiter = max(est_tokens, usage["total"])
        await ag_limiter.finalize(slot, total_for_limiter)

        logger.info(
            f"📊 [AG FINAL TOKENS] ключ=#{assigned_key} | статус={status_raw} | "
            f"зашло(in)={usage['input']} (кэш={usage['cached']}, новых={new_in}) | "
            f"вышло(out)={usage['output']} | всего(total)={usage['total']}"
        )

        if "cancel" in status_raw:
            raise RuntimeError("Google прервал выполнение задачи в песочнице (status=cancelled).")

        if "fail" in status_raw:
            err_obj = str(getattr(interaction, "error", "Unknown error"))
            raise Exception(f"Задача Antigravity завершилась ошибкой: {err_obj}")

        raw_text = _extract_text_any(interaction, streamed_fallback=streamed_text)
        output_text, sync_matches = _extract_and_normalize_sync_tags(raw_text)

        if "incomplete" in status_raw and not sync_matches and env_id and selected_files:
            try:
                env_files = await list_environment_files(api_keys, env_id, key_index=assigned_key)
                selected_norm = {
                    re.sub(r"^(\./)+", "", f.strip().replace("\\", "/")).lstrip("/")
                    for f in selected_files if f and f.strip()
                }
                selected_names = {os.path.basename(s) for s in selected_norm}
                auto_sync = []
                for ef in env_files:
                    ep = re.sub(r"^(\./)+", "", str(ef.get("path") or "").lstrip("/"))
                    if ep.startswith("workspace/"):
                        ep = ep[len("workspace/"):]
                    if ep in selected_norm or os.path.basename(ep) in selected_names:
                        if ep not in auto_sync:
                            auto_sync.append(ep)
                if auto_sync:
                    sync_matches = auto_sync
                    output_text = (output_text.rstrip() + "\n\n" + "\n".join(f"[SYNC: {p}]" for p in auto_sync)).strip()
            except Exception as auto_e:
                logger.warning(f"⚠️ [INCOMPLETE AUTO-SYNC ERR] {auto_e}")

        file_urls = {}
        if sync_matches and env_id:
            logger.info(f"📦 [SYNC LIST] Запрошена синхронизация: {sync_matches}")
            save_dir = os.path.abspath(os.path.join('/opt/friday/storage/uploads', os.path.basename(chat_uid or 'common')))
            os.makedirs(save_dir, exist_ok=True)
            env_client = create_genai_client(api_keys, assigned_key, timeout=180000)

            async def try_download_path(path_to_get: str):
                for retry in range(2):
                    try:
                        dl_client = create_genai_client(api_keys, assigned_key, timeout=60000, with_retry_opt=False)
                        file_data = await asyncio.wait_for(
                            asyncio.to_thread(dl_client.environments.files.download, environment=env_id, path=path_to_get),
                            timeout=60.0
                        )
                        content = file_data.read() if hasattr(file_data, "read") else file_data
                        if hasattr(content, "content"):
                            content = content.content
                        return content
                    except Exception as dl_err:
                        err_txt = str(dl_err).lower()
                        if "404" in err_txt or "not_found" in err_txt or "not found" in err_txt:
                            return None
                        logger.warning(f"⚠️ [SYNC RETRY {retry+1}/2] '{path_to_get}': {type(dl_err).__name__}: {dl_err}")
                        await asyncio.sleep(1.5)
                return None

            real_paths_in_env = None
            for sync_path in sync_matches:
                clean_path = _sanitize_sync_path(sync_path)
                if not clean_path:
                    continue

                local_server_path = os.path.abspath(os.path.join(save_dir, clean_path))
                if not local_server_path.startswith(save_dir + os.sep) or os.path.isdir(local_server_path):
                    continue

                fname = os.path.basename(clean_path)
                download_success = False

                for cp in [f"workspace/{clean_path}", clean_path]:
                    content = await try_download_path(cp)
                    if content is not None:
                        os.makedirs(os.path.dirname(local_server_path), exist_ok=True)
                        with open(local_server_path, 'wb') as sf:
                            sf.write(content)
                        file_url = f"https://friday-assistant.ru/uploads/{os.path.basename(chat_uid or 'common')}/{urllib.parse.quote(clean_path)}?v={int(time.time())}"
                        file_urls[clean_path] = file_url
                        download_success = True
                        break

                if download_success:
                    continue

                if real_paths_in_env is None:
                    real_paths_in_env = []
                    try:
                        top_resp = await asyncio.wait_for(
                            asyncio.to_thread(env_client.environments.files.list, environment=env_id, path="", recursive=False),
                            timeout=30.0
                        )
                        for top_f in getattr(top_resp, "files", []) or []:
                            tname = (getattr(top_f, "name", "") or "").strip("/")
                            ttype = str(getattr(top_f, "type", "")).lower()
                            if not tname or tname in SYS_DIRS:
                                continue
                            if "dir" not in ttype:
                                real_paths_in_env.append(getattr(top_f, "path", None) or tname)
                            else:
                                sub_resp = await asyncio.wait_for(
                                    asyncio.to_thread(env_client.environments.files.list, environment=env_id, path=tname, recursive=True),
                                    timeout=30.0
                                )
                                for sf_item in getattr(sub_resp, "files", []) or []:
                                    if "dir" in str(getattr(sf_item, "type", "")).lower():
                                        continue
                                    spath = getattr(sf_item, "path", None) or getattr(sf_item, "name", "")
                                    if spath and "__pycache__" not in spath:
                                        real_paths_in_env.append(spath)
                    except Exception as le:
                        logger.warning(f"⚠️ [ENV FILES] Ошибка поиска: {le}")

                for rp in real_paths_in_env:
                    norm_rp = rp.lstrip("/")
                    if norm_rp == clean_path or norm_rp.endswith("/" + clean_path) or os.path.basename(norm_rp) == fname:
                        content = await try_download_path(rp)
                        if content is not None:
                            os.makedirs(os.path.dirname(local_server_path), exist_ok=True)
                            with open(local_server_path, 'wb') as sf:
                                sf.write(content)
                            file_url = f"https://friday-assistant.ru/uploads/{os.path.basename(chat_uid or 'common')}/{urllib.parse.quote(clean_path)}?v={int(time.time())}"
                            file_urls[clean_path] = file_url
                            break

        key_pool.mark_success(assigned_key, model=model_tag)

        if "incomplete" in status_raw:
            if output_text.strip():
                output_text += f"\n\n[Достигнут лимит {max_total_tokens} токенов — часть работы сохранена.]"
            else:
                raise BudgetExhaustedError(
                    f"Агент упёрся в лимит {max_total_tokens} некэшированных токенов до выдачи ответа."
                )

        return {
            "text": output_text,
            "files": {},
            "file_urls": file_urls,
            "environment_id": env_id,
            "interaction_id": None,
            "assigned_key": assigned_key,
            "usage": usage,
        }

    except asyncio.CancelledError:
        if new_interaction_id:
            await _cancel_interaction_safe(api_keys, assigned_key, new_interaction_id)
        raise
    except TimeoutError as te:
        logger.warning(f"⏰ [ТАЙМАУТ] Ключ #{assigned_key} ({ag_model}): {te}")
        if new_interaction_id:
            await _cancel_interaction_safe(api_keys, assigned_key, new_interaction_id)
        raise
    except Exception as e:
        key_pool.register_key_error(assigned_key, model=model_tag, exc=e)
        raise


async def list_environment_files(api_keys: list[str], env_id: str, key_index: int = 0):
    client = create_genai_client(api_keys, key_index or 0, timeout=30000)
    items = []
    top_resp = await asyncio.wait_for(
        asyncio.to_thread(client.environments.files.list, environment=env_id, path="", recursive=False),
        timeout=30.0
    )
    for top_f in getattr(top_resp, "files", []) or []:
        tname = (getattr(top_f, "name", "") or "").strip("/")
        ttype = str(getattr(top_f, "type", "")).lower()
        if not tname or tname in SYS_DIRS:
            continue
        if "dir" not in ttype:
            spath = getattr(top_f, "path", None) or tname
            items.append({"path": spath, "name": os.path.basename(spath), "size": int(getattr(top_f, "size_bytes", 0) or 0)})
        else:
            sub_resp = await asyncio.wait_for(
                asyncio.to_thread(client.environments.files.list, environment=env_id, path=tname, recursive=True),
                timeout=30.0
            )
            for sf_item in getattr(sub_resp, "files", []) or []:
                if "dir" in str(getattr(sf_item, "type", "")).lower():
                    continue
                spath = getattr(sf_item, "path", None) or getattr(sf_item, "name", "")
                if not spath or "__pycache__" in spath:
                    continue
                items.append({"path": spath, "name": os.path.basename(spath), "size": int(getattr(sf_item, "size_bytes", 0) or 0)})
    items.sort(key=lambda x: x["path"])
    return items


async def download_environment_file(api_keys: list[str], env_id: str, remote_path: str, chat_uid: str, key_index: int = 0):
    dl_client = create_genai_client(api_keys, key_index or 0, timeout=60000, with_retry_opt=False)
    file_data = await asyncio.wait_for(
        asyncio.to_thread(dl_client.environments.files.download, environment=env_id, path=remote_path),
        timeout=60.0
    )
    content = file_data.read() if hasattr(file_data, "read") else file_data
    if hasattr(content, "content"):
        content = content.content

    clean_rel = remote_path.strip().replace("\\", "/").lstrip("/")
    if clean_rel.startswith("workspace/"):
        clean_rel = clean_rel[len("workspace/"):]
    clean_rel = os.path.normpath(clean_rel).replace("\\", "/").lstrip("./")
    if clean_rel.startswith(".."):
        clean_rel = os.path.basename(clean_rel)

    safe_uid = os.path.basename(chat_uid or 'common')
    save_dir = os.path.abspath(os.path.join('/opt/friday/storage/uploads', safe_uid))
    local_server_path = os.path.abspath(os.path.join(save_dir, clean_rel))
    if not local_server_path.startswith(save_dir + os.sep):
        raise ValueError("Invalid file path")
    os.makedirs(os.path.dirname(local_server_path), exist_ok=True)
    with open(local_server_path, 'wb') as sf:
        sf.write(content)

    file_url = f"https://friday-assistant.ru/uploads/{safe_uid}/{urllib.parse.quote(clean_rel)}?v={int(time.time())}"
    return {"path": clean_rel, "url": file_url, "size": len(content)}


async def upload_file_to_environment(api_keys: list[str], env_id: str, local_server_path: str, remote_path: str, key_index: int = 0):
    client = create_genai_client(api_keys, key_index or 0, timeout=180000)
    clean_remote = remote_path.strip().replace("\\", "/").lstrip("/")

    def _do_upload():
        with open(local_server_path, "rb") as f:
            return client.environments.files.upload(
                environment=env_id,
                path=clean_remote,
                file=f,
                overwrite=True
            )

    await asyncio.wait_for(asyncio.to_thread(_do_upload), timeout=180.0)
    return clean_remote