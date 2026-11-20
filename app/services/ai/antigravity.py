# -*- coding: utf-8 -*-
import os
import re
import time
import asyncio
import logging
import urllib.parse
from .key_pool import GeminiKeyPool, create_genai_client, extract_interaction_text, extract_usage_tokens
from .prompts import AGENT_SYSTEM_INSTRUCTION
from .router import format_stateless_prompt

logger = logging.getLogger("AI_Service")
SYS_DIRS = {".", "..", "usr", "bin", "lib", "lib64", "lib32", "libx32", "sbin", "proc", "sys", "dev", "etc", "var", "boot", "run", "srv", "snap", "media", "mnt"}


async def run_antigravity_agent(
    key_pool: GeminiKeyPool,
    api_keys: list[str],
    prompt_text: str,
    status_callback=None,
    previous_interaction_id: str = None,
    environment_id: str = None,
    assigned_key: int = None,
    chat_uid: str = None,
    recent_history: list = None,
    summary: str = None,
    force_reset_chain: bool = False
):
    total_keys = len(api_keys)

    model_tag = "antigravity"
    tried: set[int] = set()

    if assigned_key is None or not key_pool.is_usable(assigned_key, model=model_tag):
        assigned_key = key_pool.get_best_key(model=model_tag)
        previous_interaction_id = None
        environment_id = None

    if force_reset_chain:
        previous_interaction_id = None

    max_attempts = min(total_keys, 12) if total_keys > 0 else 3
    attempt = 0

    while attempt < max_attempts:
        tried.add(assigned_key)
        try:
            if status_callback:
                await status_callback({"status": f"Подключение к контейнеру (ключ #{assigned_key})..."})

            tools = [
                {"type": "code_execution"},
                {"type": "google_search"},
                {"type": "url_context"},
            ]

            kwargs = {
                "agent": "antigravity-preview-09-2026",
                "background": True,
                "system_instruction": AGENT_SYSTEM_INSTRUCTION,
                "tools": tools,
                "agent_config": {
                    "type": "antigravity",
                    "model": "gemini-4-hydrogen",
                }
            }

            if previous_interaction_id and environment_id:
                kwargs["previous_interaction_id"] = previous_interaction_id
                kwargs["environment"] = environment_id
                kwargs["input"] = prompt_text
                logger.info(f"🔗 [GOOGLE CALL] Продолжение среды {environment_id[:10]} | Ключ #{assigned_key}")
            elif environment_id:
                # Мягкий сброс раздутой цепочки с сохранением Linux-контейнера и его файлов!
                kwargs["environment"] = environment_id
                kwargs["input"] = format_stateless_prompt(prompt_text, summary=summary, recent_history=recent_history)
                logger.info(f"♻️ [GOOGLE CALL] Сжатие контекста в среде {environment_id[:10]} | Ключ #{assigned_key}")
            else:
                kwargs["environment"] = {
                    "type": "remote",
                    "sources": [
                        {
                            "type": "inline",
                            "target": ".agents/AGENTS.md",
                            "content": AGENT_SYSTEM_INSTRUCTION
                        }
                    ]
                }
                kwargs["input"] = format_stateless_prompt(prompt_text, summary=summary, recent_history=recent_history)
                logger.info(f"✨ [GOOGLE CALL] Новый контейнер | Ключ #{assigned_key}")

            create_client = create_genai_client(api_keys, assigned_key, timeout=25000)
            interaction = await asyncio.wait_for(
                asyncio.to_thread(create_client.interactions.create, **kwargs),
                timeout=30.0
            )

            new_interaction_id = interaction.id
            env_id = interaction.environment_id or environment_id
            logger.info(f"🆔 [GOOGLE CREATED] interaction_id={new_interaction_id} | env_id={env_id}")

            if status_callback:
                await status_callback({"status": "Агент думает (контейнер запущен)...", "interaction_id": new_interaction_id})

            poll_count = 0
            consecutive_poll_errors = 0
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
                    key_pool.mark_exhausted(assigned_key, duration_seconds=1800, model=model_tag)  # или 10800 при 429/quota
                    assigned_key = key_pool.get_best_key(model=model_tag, exclude_indices=tried)
                    consecutive_poll_errors += 1
                    logger.warning(f"⚠️ [POLL RETRY {consecutive_poll_errors}/10] Ключ #{assigned_key}: {type(poll_err).__name__}: {poll_err}")
                    if consecutive_poll_errors >= 10:
                        raise Exception(f"Потеряна связь при опросе статуса: {poll_err}")
                    poll_client = create_genai_client(api_keys, assigned_key, timeout=180000)
                    if status_callback:
                        await status_callback({"status": f"Переподключение к проверке статуса ({consecutive_poll_errors}/10)..."})
                    await asyncio.sleep(2.0)
                    continue

                status_raw = str(getattr(interaction, 'status', '')).lower().strip()
                if poll_count % 5 == 0:
                    logger.info(f"⏳ [ANTIGRAVITY POLLING] Статус: {status_raw} (шаг {poll_count})")

                if status_callback:
                    await status_callback({"status": f"Агент думает ({poll_count * 2.5:.0f}с)..."})

                if "complete" in status_raw or "fail" in status_raw or "cancel" in status_raw:
                    logger.info(f"🏁 [ANTIGRAVITY DONE] Статус: {status_raw}")
                    break

            if "fail" in str(getattr(interaction, 'status', '')).lower():
                key_pool.mark_exhausted(assigned_key, duration_seconds=1800, model=model_tag)  # или 10800 при 429/quota
                assigned_key = key_pool.get_best_key(model=model_tag, exclude_indices=tried)
                raise Exception(f"Задача Antigravity завершилась ошибкой: {getattr(interaction, 'error', 'Unknown error')}")

            usage = extract_usage_tokens(interaction)
            logger.info(f"📊 [ANTIGRAVITY USAGE] Ключ #{assigned_key} | in={usage['input']} cached={usage['cached']} out={usage['output']} total={usage['total']}")

            output_text = extract_interaction_text(interaction)
            sync_matches = re.findall(r"\[SYNC:\s*([^\]]+)\]", output_text)
            file_urls = {}

            if sync_matches:
                logger.info(f"📦 [SYNC LIST] Запрошена синхронизация: {sync_matches}")
                save_dir = os.path.join('/opt/friday/storage/uploads', os.path.basename(chat_uid or 'common'))
                os.makedirs(save_dir, exist_ok=True)
                client = create_genai_client(api_keys, assigned_key, timeout=180000)

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
                                asyncio.to_thread(client.environments.files.list, environment=env_id, path="", recursive=False),
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
                                        asyncio.to_thread(client.environments.files.list, environment=env_id, path=tname, recursive=True),
                                        timeout=30.0
                                    )
                                    for sf_item in getattr(sub_resp, "files", []) or []:
                                        if "dir" in str(getattr(sf_item, "type", "")).lower():
                                            continue
                                        spath = getattr(sf_item, "path", None) or getattr(sf_item, "name", "")
                                        if spath and "__pycache__" not in spath:
                                            real_paths_in_env.append(spath)
                        except Exception as le:
                            key_pool.mark_exhausted(assigned_key, duration_seconds=1800, model=model_tag)  # или 10800 при 429/quota
                            assigned_key = key_pool.get_best_key(model=model_tag, exclude_indices=tried)
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
                                download_success = True
                                break
            key_pool.mark_success(assigned_key, model=model_tag)
            return {
                "text": output_text,
                "files": {},
                "file_urls": file_urls,
                "environment_id": env_id,
                "interaction_id": new_interaction_id,
                "assigned_key": assigned_key,
                "usage": usage,
            }

        except (asyncio.TimeoutError, TimeoutError):
            key_pool.mark_exhausted(assigned_key, duration_seconds=1800, model=model_tag)  # или 10800 при 429/quota
            assigned_key = key_pool.get_best_key(model=model_tag, exclude_indices=tried)
            logger.warning(f"⏰ [ТАЙМАУТ] Ключ #{assigned_key} не ответил за 30с.")
            key_pool.mark_exhausted(assigned_key, duration_seconds=1800)
            assigned_key = key_pool.get_next_key(exclude_index=assigned_key)
            previous_interaction_id = None
            environment_id = None
            attempt += 1

        except Exception as e:
            key_pool.mark_exhausted(assigned_key, duration_seconds=1800, model=model_tag)  # или 10800 при 429/quota
            assigned_key = key_pool.get_best_key(model=model_tag, exclude_indices=tried)
            err_lower = str(e).lower()
            logger.warning(f"[Antigravity Ошибка на ключе #{assigned_key}]: {e}")

            if "timed out" in err_lower or "timeout" in err_lower:
                key_pool.mark_exhausted(assigned_key, duration_seconds=1800)
                assigned_key = key_pool.get_next_key(exclude_index=assigned_key)
                previous_interaction_id = None
                environment_id = None
                attempt += 1
                continue

            if ("not found" in err_lower or "permission" in err_lower or "403" in err_lower or "404" in err_lower) and (previous_interaction_id or environment_id):
                logger.info(f"🔄 Сброс чужой среды #{environment_id} для ключа #{assigned_key}.")
                previous_interaction_id = None
                environment_id = None
                attempt += 1
                continue

            if any(net_err in err_lower for net_err in ["eof", "ssl", "connection reset", "connection refused", "broken pipe"]):
                await asyncio.sleep(2)
                attempt += 1
                continue

            key_pool.mark_exhausted(assigned_key, duration_seconds=3600)
            assigned_key = key_pool.get_next_key(exclude_index=assigned_key)
            previous_interaction_id = None
            environment_id = None
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
