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


class AsyncAntigravityLimiter:
    """Асинхронное скользящее окно RPM и TPM (60 сек) для квоты Antigravity (100K TPM)."""

    def __init__(self, max_rpm: int = 50, max_tpm: int = 90_000, min_headroom: int = 25_000):
        self.max_rpm = max_rpm
        self.max_tpm = max_tpm
        self.min_headroom = min_headroom
        self._lock = asyncio.Lock()
        self._entries: deque[list] = deque()  # [timestamp, tokens]

    def _prune(self, now: float) -> None:
        cutoff = now - 60.0
        while self._entries and self._entries[0][0] < cutoff:
            self._entries.popleft()

    async def acquire(self, estimated_tokens: int, status_callback=None) -> list:
        need = max(self.min_headroom, estimated_tokens)
        while True:
            sleep_for = 0.0
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

            logger.info(f"⏳ [AG LIMITER] Окно TPM заполнено, ждём {sleep_for:.1f}с...")
            if status_callback:
                await status_callback({"status": f"Ожидание окна лимита TPM ({sleep_for:.0f}с)..."})
            await asyncio.sleep(sleep_for)

    async def finalize(self, slot: list, actual_tokens: int) -> None:
        if actual_tokens <= 0:
            return
        async with self._lock:
            slot[0] = time.monotonic()
            slot[1] = actual_tokens


ag_limiter = AsyncAntigravityLimiter(max_rpm=50, max_tpm=90_000, min_headroom=25_000)


def _is_dead_project_error(err_str: str) -> bool:
    return (
        "'quota_limit_value': '0'" in err_str
        or "ApiRequestsPerMinutePerProjectPerRegion" in err_str
        or "API_KEY_INVALID" in err_str
        or "401" in err_str
    )


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
    max_total_tokens: int = 60_000,
    **kwargs,
):
    total_keys = len(api_keys)
    model_tag = "antigravity"
    tried: set[int] = set()

    if assigned_key is None or not key_pool.is_usable(assigned_key, model=model_tag):
        assigned_key = key_pool.get_best_key(model=model_tag)

    max_attempts = min(total_keys, 6) if total_keys > 0 else 2
    attempt = 0
    ag_model = normalize_model_name(get_models_config()["antigravity_model"])

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

    while attempt < max_attempts:
        tried.add(assigned_key)
        slot = await ag_limiter.acquire(est_tokens, status_callback=status_callback)
        try:
            if status_callback:
                await status_callback({"status": f"Запуск чистой песочницы ({ag_model}, ключ #{assigned_key})..."})

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
                await status_callback({"status": "Агент работает в песочнице...", "interaction_id": new_interaction_id})

            poll_count = 0
            consecutive_poll_errors = 0
            poll_client = create_genai_client(api_keys, assigned_key, timeout=180000)

            while True:
                await asyncio.sleep(2.5)
                poll_count += 1
                try:
                    # Опрос ВСЕГДА тем же ключом, который создал interaction_id!
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

                status_raw = str(getattr(interaction, "status", "")).lower().strip()
                if poll_count % 5 == 0:
                    logger.info(f"⏳ [ANTIGRAVITY POLLING] Статус: {status_raw} (шаг {poll_count})")

                if status_callback:
                    await status_callback({"status": f"Агент работает ({poll_count * 2.5:.0f}с)..."})

                if "complete" in status_raw or "fail" in status_raw or "cancel" in status_raw:
                    logger.info(f"🏁 [ANTIGRAVITY DONE] Статус: {status_raw}")
                    break

            usage = extract_usage_tokens(interaction)
            await ag_limiter.finalize(slot, usage["total"] or est_tokens)
            logger.info(f"📊 [ANTIGRAVITY USAGE] Ключ #{assigned_key} | in={usage['input']} cached={usage['cached']} out={usage['output']} total={usage['total']}")

            if "fail" in str(getattr(interaction, "status", "")).lower():
                err_obj = str(getattr(interaction, "error", "Unknown error"))
                raise Exception(f"Задача Antigravity завершилась ошибкой: {err_obj}")

            output_text = extract_interaction_text(interaction)
            sync_matches = re.findall(r"\[SYNC:\s*([^\]]+)\]", output_text)
            file_urls = {}

            if sync_matches and env_id:
                logger.info(f"📦 [SYNC LIST] Запрошена синхронизация: {sync_matches}")
                save_dir = os.path.join('/opt/friday/storage/uploads', os.path.basename(chat_uid or 'common'))
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
                    clean_path = sync_path.strip().replace("\\", "/").lstrip("./")
                    fname = os.path.basename(clean_path)
                    download_success = False

                    for cp in [f"workspace/{clean_path}", clean_path]:
                        content = await try_download_path(cp)
                        if content is not None:
                            local_server_path = os.path.join(save_dir, clean_path)
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
                                local_server_path = os.path.join(save_dir, clean_path)
                                os.makedirs(os.path.dirname(local_server_path), exist_ok=True)
                                with open(local_server_path, 'wb') as sf:
                                    sf.write(content)
                                file_url = f"https://friday-assistant.ru/uploads/{os.path.basename(chat_uid or 'common')}/{urllib.parse.quote(clean_path)}?v={int(time.time())}"
                                file_urls[clean_path] = file_url
                                break

            key_pool.mark_success(assigned_key, model=model_tag)
            return {
                "text": output_text,
                "files": {},
                "file_urls": file_urls,
                "environment_id": env_id,
                "interaction_id": None,  # Никогда не продолжаем цепочку
                "assigned_key": assigned_key,
                "usage": usage,
            }

        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, TimeoutError):
            logger.warning(f"⏰ [ТАЙМАУТ] Ключ #{assigned_key} не ответил за 30с.")
            key_pool.mark_exhausted(assigned_key, duration_seconds=1800, model=model_tag)
            assigned_key = key_pool.get_best_key(model=model_tag, exclude_indices=tried)
            attempt += 1
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

            attempt += 1

    raise Exception("Все доступные ключи не отвечают или исчерпали квоту.")


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