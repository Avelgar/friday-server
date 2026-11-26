# -*- coding: utf-8 -*-
import os
import re
import time
import asyncio
import logging
import urllib.parse
from collections import deque
from .key_pool import GeminiKeyPool, create_genai_client, extract_interaction_text, extract_usage_tokens
from .prompts import AGENT_SYSTEM_INSTRUCTION
from .router import format_stateless_prompt
from .model_config import get_models_config, normalize_model_name

logger = logging.getLogger("AI_Service")
SYS_DIRS = {".", "..", "usr", "bin", "lib", "lib64", "lib32", "libx32", "sbin", "proc", "sys", "dev", "etc", "var", "boot", "run", "srv", "snap", "media", "mnt"}


class BudgetExhaustedError(RuntimeError):
    """Остановка по нашему потолку max_total_tokens (ключ здоров, ретраить не нужно)."""


class AsyncAntigravityLimiter:
    """Асинхронное скользящее окно RPM и TPM (60 сек) по некэшированным токенам."""

    def __init__(self, max_rpm: int = 50, max_tpm: int = 90_000, min_headroom: int = 15_000):
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
                await status_callback({"status": f"Ожидание окна лимита TPM ({sleep_for:.0f}с)..."})
            await asyncio.sleep(sleep_for)

    async def finalize(self, slot: list, actual_billable_tokens: int) -> None:
        if actual_billable_tokens <= 0:
            return
        async with self._lock:
            slot[0] = time.monotonic()
            slot[1] = actual_billable_tokens


ag_limiter = AsyncAntigravityLimiter(max_rpm=50, max_tpm=90_000, min_headroom=15_000)


def _is_dead_project_error(err_str: str) -> bool:
    return (
        "'quota_limit_value': '0'" in err_str
        or "ApiRequestsPerMinutePerProjectPerRegion" in err_str
        or "API_KEY_INVALID" in err_str
        or "401" in err_str
    )


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
    need_network: bool = False,
    max_total_tokens: int = 85_000,
    **kwargs,
):
    total_keys = len(api_keys)
    model_tag = "antigravity"
    tried: set[int] = set()

    if assigned_key is None or not key_pool.is_usable(assigned_key, model=model_tag):
        assigned_key = key_pool.get_best_key(model=model_tag)

    primary_ag_model = normalize_model_name(get_models_config()["antigravity_model"])
    ag_models_queue = [primary_ag_model]
    if primary_ag_model != "gemini-3.5-flash-lite":
        ag_models_queue.append("gemini-3.5-flash-lite")

    full_input = format_stateless_prompt(
        prompt_text=prompt_text,
        recent_history=recent_history,
        project_tree=project_tree,
        file_manifest=file_manifest,
        selected_files=selected_files,
    )
    est_tokens = max(2000, len(full_input) // 3)

    tools = [{"type": "code_execution"}]
    if need_network:
        tools.extend([{"type": "google_search"}, {"type": "url_context"}])

    max_attempts = min(max(total_keys, 2), 4)
    model_idx = 0

    for _ in range(max_attempts):
        ag_model = ag_models_queue[min(model_idx, len(ag_models_queue) - 1)]
        tried.add(assigned_key)
        slot = await ag_limiter.acquire(est_tokens, status_callback=status_callback)
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
            logger.info(f"✨ [GOOGLE CALL] Новая ВМ | модель={ag_model} | max_tokens={max_total_tokens} | network={need_network} | Ключ #{assigned_key}")

            create_client = create_genai_client(api_keys, assigned_key, timeout=25000)
            interaction = await asyncio.wait_for(
                asyncio.to_thread(create_client.interactions.create, **req_kwargs),
                timeout=30.0
            )

            new_interaction_id = interaction.id
            env_id = interaction.environment_id
            logger.info(f"🆔 [GOOGLE CREATED] interaction_id={new_interaction_id} | env_id={env_id}")

            if status_callback:
                await status_callback({"status": f"Агент работает ({ag_model})...", "interaction_id": new_interaction_id})

            poll_count = 0
            consecutive_poll_errors = 0
            logged_steps = 0
            poll_client = create_genai_client(api_keys, assigned_key, timeout=180000)

            while True:
                await asyncio.sleep(2.5)
                poll_count += 1
                try:
                    interaction = await asyncio.wait_for(
                        asyncio.to_thread(poll_client.interactions.get, id=new_interaction_id),
                        timeout=25.0
                    )
                    consecutive_poll_errors = 0
                except asyncio.CancelledError:
                    raise
                except Exception as poll_err:
                    consecutive_poll_errors += 1
                    logger.warning(f"⚠️ [POLL RETRY {consecutive_poll_errors}/8] Ключ #{assigned_key}: {type(poll_err).__name__}: {poll_err}")
                    if consecutive_poll_errors >= 8:
                        raise Exception(f"Потеряна связь при опросе статуса: {poll_err}")
                    await asyncio.sleep(2.0)
                    continue

                steps = getattr(interaction, "steps", None) or []
                while logged_steps < len(steps):
                    logger.info(f"🧩 [ANTIGRAVITY STEP #{logged_steps + 1}] {_describe_step(steps[logged_steps])}")
                    logged_steps += 1

                status_raw = str(getattr(interaction, "status", "")).lower().strip()
                if poll_count % 5 == 0:
                    logger.info(f"⏳ [ANTIGRAVITY POLLING] Статус: {status_raw} (шаг {poll_count}, {poll_count * 2.5:.0f}с)")

                if status_callback:
                    await status_callback({"status": f"Агент работает ({poll_count * 2.5:.0f}с, шагов: {len(steps)})..."})

                if status_raw in ("completed", "incomplete", "failed", "cancelled") or any(k in status_raw for k in ("complete", "fail", "cancel")):
                    logger.info(f"🏁 [ANTIGRAVITY DONE] Статус: {status_raw}")
                    break

                if poll_count >= 72:
                    raise TimeoutError(f"Песочница ({ag_model}) зависла дольше 180 секунд.")

            usage = extract_usage_tokens(interaction)
            billable_tokens = max(est_tokens, max(0, usage["total"] - usage["cached"]))
            await ag_limiter.finalize(slot, billable_tokens)
            logger.info(
                f"📊 [ANTIGRAVITY USAGE] Ключ #{assigned_key} | in={usage['input']} cached={usage['cached']} "
                f"out={usage['output']} total={usage['total']} (некэш≈{billable_tokens})"
            )

            if "cancel" in status_raw:
                if model_idx + 1 < len(ag_models_queue):
                    logger.warning(f"⚠️ [SANDBOX CANCELLED] {ag_model} отменён Google; повторяем на {ag_models_queue[model_idx + 1]}...")
                    model_idx += 1
                    continue
                raise RuntimeError("Google прервал выполнение задачи в песочнице (status=cancelled).")

            if "fail" in status_raw:
                err_obj = str(getattr(interaction, "error", "Unknown error"))
                raise Exception(f"Задача Antigravity завершилась ошибкой: {err_obj}")

            output_text, sync_matches = _extract_and_normalize_sync_tags(extract_interaction_text(interaction))

            # Если агент упёрся в лимит до вывода тега [SYNC: ...], забираем выбранные файлы из песочницы сами
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
                        logger.warning(f"⚠️ [SYNC SKIP] Пропущен некорректный путь: '{sync_path}'")
                        continue

                    local_server_path = os.path.abspath(os.path.join(save_dir, clean_path))
                    if not local_server_path.startswith(save_dir + os.sep) or os.path.isdir(local_server_path):
                        logger.warning(f"⚠️ [SYNC SKIP] Небезопасный или директория: '{local_server_path}'")
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
                    output_text += f"\n\n[Достигнут лимит {max_total_tokens} токенов на 1 запуск — часть работы сохранена.]"
                else:
                    raise BudgetExhaustedError(
                        f"Агент упёрся в лимит {max_total_tokens} некэшированных токенов до выдачи ответа. "
                        f"Передай 1-2 файла вместо всех сразу или разбей задачу на этапы."
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

        except BudgetExhaustedError:
            raise
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, TimeoutError) as te:
            logger.warning(f"⏰ [ТАЙМАУТ] Ключ #{assigned_key} ({ag_model}): {te}")
            if "зависла дольше" in str(te) and model_idx + 1 < len(ag_models_queue):
                model_idx += 1
                continue
            key_pool.mark_exhausted(assigned_key, duration_seconds=1800, model=model_tag)
            assigned_key = key_pool.get_best_key(model=model_tag, exclude_indices=tried)
        except Exception as e:
            err_str = str(e)
            err_lower = err_str.lower()
            logger.warning(f"⚠️ [Antigravity Ошибка на ключе #{assigned_key}]: {e}")

            if _is_dead_project_error(err_str):
                key_pool.mark_dead_all_models(assigned_key)
                assigned_key = key_pool.get_best_key(model=model_tag, exclude_indices=tried)
            elif "429" in err_lower or "resource_exhausted" in err_lower or "quota" in err_lower:
                key_pool.mark_exhausted(assigned_key, duration_seconds=10800, model=model_tag)
                assigned_key = key_pool.get_best_key(model=model_tag, exclude_indices=tried)
            elif any(net_err in err_lower for net_err in ["eof", "ssl", "connection reset", "connection refused", "broken pipe"]):
                await asyncio.sleep(2.0)
            else:
                key_pool.mark_exhausted(assigned_key, duration_seconds=1800, model=model_tag)
                assigned_key = key_pool.get_best_key(model=model_tag, exclude_indices=tried)

    raise Exception("Не удалось выполнить задачу в Antigravity.")


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