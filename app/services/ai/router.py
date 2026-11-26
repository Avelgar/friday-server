# -*- coding: utf-8 -*-
import re
import asyncio
import logging
from google.genai import types
from .key_pool import GeminiKeyPool, create_genai_client
from .model_config import get_models_config, to_api_model
from .prompts import DIRECT_SYSTEM_INSTRUCTION

logger = logging.getLogger("AI_Service")

DIRECT_HISTORY_CHAR_BUDGET = 12000
ANTIGRAVITY_HISTORY_CHAR_BUDGET = 9000
CALL_PREFIXES = ("[CALL_ANTIGRAVITY", "[ESCALATE_ANTIGRAVITY")


def _compress_history_entries(
    recent_history: list | None,
    char_budget: int,
    max_items: int = 12,
    user_agent_cap: int = 1800,
    latest_tool_cap: int = 3000,
    old_tool_cap: int = 450,
) -> list[tuple[str, str]]:
    """Набирает историю с конца до жёсткого лимита символов, сжимая старые tool-выводы."""
    if not recent_history:
        return []

    tail = recent_history[-max_items:]
    collected: list[tuple[str, str]] = []
    used_chars = 0
    seen_latest_tool = False

    for m in reversed(tail):
        r = m.get("role") or "user"
        txt = (m.get("text") or "").strip()
        if not txt:
            continue

        if r in ("tool", "system"):
            cap = old_tool_cap if seen_latest_tool else latest_tool_cap
            seen_latest_tool = True
        else:
            cap = user_agent_cap

        if len(txt) > cap:
            txt = txt[:cap] + "\n...[обрезано]..."

        if used_chars + len(txt) > char_budget and collected:
            break

        collected.append((r, txt))
        used_chars += len(txt)

    collected.reverse()
    return collected


def format_manifest_block(file_manifest: list | None, selected_files: list | None = None, with_urls: bool = True) -> str:
    if not file_manifest:
        return "Файлов на сервере пока нет."

    selected_norm = {
        re.sub(r"^(\./)+", "", f.strip().replace("\\", "/")).lstrip("/")
        for f in (selected_files or [])
        if f and f.strip()
    }

    ordered = list(file_manifest)
    if selected_norm:
        priority = [f for f in ordered if f["path"] in selected_norm]
        others = [f for f in ordered if f["path"] not in selected_norm]
        ordered = priority + others[:6]
    else:
        ordered = ordered[:15]

    lines = []
    for f in ordered:
        size_kb = f["size"] / 1024.0
        size_str = f"{size_kb:.1f} КБ" if size_kb >= 0.1 else f"{f['size']} Б"
        lines_info = f", ~{f['lines']} строк" if f.get("lines") is not None else ""
        mark = " [ВЫБРАН ДЛЯ ЗАДАЧИ]" if f["path"] in selected_norm else ""
        if with_urls:
            lines.append(f"• {f['path']}{mark} ({size_str}{lines_info}) | curl: {f['url']}")
        else:
            lines.append(f"• {f['path']}{mark} ({size_str}{lines_info})")
    return "\n".join(lines)


def format_stateless_prompt(
    prompt_text: str,
    recent_history: list = None,
    project_tree: str = None,
    file_manifest: list = None,
    selected_files: list = None,
) -> str:
    blocks = []

    if project_tree and project_tree.strip():
        tree_txt = project_tree.strip()
        if len(tree_txt) > 4500:
            tree_txt = tree_txt[:4500] + "\n...[дерево обрезано]..."
        blocks.append(f"# Структура проекта (с сервера):\n{tree_txt}")

    manifest_txt = format_manifest_block(file_manifest, selected_files=selected_files, with_urls=True)
    blocks.append(f"# Файлы на сервере (скачивай через curl -sL -m 20 --create-dirs -o /workspace/<путь> \"<url>\"):\n{manifest_txt}")

    entries = _compress_history_entries(recent_history, char_budget=ANTIGRAVITY_HISTORY_CHAR_BUDGET)
    if entries:
        lines = []
        for r, txt in entries:
            label = "Пользователь" if r == "user" else ("Система/IDE" if r in ("system", "tool") else "Ассистент")
            lines.append(f"[{label}]: {txt}")
        blocks.append("# Последние сообщения диалога:\n" + "\n\n".join(lines))

    blocks.append(f"# Текущая задача:\n{prompt_text}")
    return "\n\n".join(blocks)


def build_direct_contents(prompt_text: str, recent_history: list = None) -> list[types.Content]:
    turns: list[tuple[str, str]] = []
    entries = _compress_history_entries(recent_history, char_budget=DIRECT_HISTORY_CHAR_BUDGET)

    for r, txt in entries:
        if r == "agent":
            role = "model"
        elif r in ("tool", "system"):
            role = "user"
            txt = f"[Событие IDE]: {txt}"
        else:
            role = "user"

        if turns and turns[-1][0] == role:
            turns[-1] = (role, turns[-1][1] + "\n\n" + txt)
        else:
            turns.append((role, txt))

    while turns and turns[0][0] == "model":
        turns.pop(0)

    if turns and turns[-1][0] == "user":
        turns[-1] = ("user", turns[-1][1] + "\n\n" + prompt_text)
    else:
        turns.append(("user", prompt_text))

    return [
        types.Content(role=role, parts=[types.Part.from_text(text=text)])
        for role, text in turns
    ]


def parse_antigravity_call(text: str) -> dict | None:
    s = (text or "").strip()
    if "[ESCALATE_ANTIGRAVITY]" in s:
        return {"task": None, "files": [], "network": False}

    m = re.search(r"\[CALL_ANTIGRAVITY:\s*(.+?)\]", s, flags=re.DOTALL | re.IGNORECASE)
    if not m:
        return None

    body = m.group(1).strip()
    need_network = False
    files: list[str] = []

    net_m = re.search(r"\|\s*network\s*:\s*([^\|\]]+)", body, flags=re.IGNORECASE)
    if net_m:
        val = net_m.group(1).strip().lower()
        need_network = val in ("да", "yes", "true", "1", "enabled")
        body = body[:net_m.start()] + body[net_m.end():]

    files_m = re.search(r"\|\s*files\s*:\s*([^\|\]]+)", body, flags=re.IGNORECASE)
    if files_m:
        raw_files = files_m.group(1).strip()
        if raw_files.lower() not in ("нет", "none", "-", "пусто", ""):
            files = [
                re.sub(r"^(\./)+", "", f.strip().replace("\\", "/")).lstrip("/")
                for f in raw_files.split(",")
                if f.strip()
            ]
        body = body[:files_m.start()] + body[files_m.end():]

    task = body.strip(" |\n\t")
    return {"task": task or None, "files": files, "network": need_network}


def _is_dead_project_error(err_str: str) -> bool:
    return (
        "'quota_limit_value': '0'" in err_str
        or "ApiRequestsPerMinutePerProjectPerRegion" in err_str
        or "API_KEY_INVALID" in err_str
        or "401" in err_str
    )


def _could_be_call_tag(text_upper: str) -> bool:
    return any(p.startswith(text_upper) or text_upper.startswith(p) for p in CALL_PREFIXES)


async def run_direct(
    key_pool: GeminiKeyPool,
    api_keys: list[str],
    prompt_text: str,
    recent_history: list = None,
    project_tree: str = None,
    file_manifest: list = None,
    status_callback=None,
    chunk_callback=None,
    assigned_key: int = None,
    **kwargs,
):
    contents = build_direct_contents(prompt_text, recent_history=recent_history)

    context_sections = [DIRECT_SYSTEM_INSTRUCTION]
    if project_tree and project_tree.strip():
        tree_txt = project_tree.strip()
        if len(tree_txt) > 5000:
            tree_txt = tree_txt[:5000] + "\n...[дерево обрезано]..."
        context_sections.append(f"ДЕРЕВО ПРОЕКТА (сохранено на сервере):\n{tree_txt}")
    else:
        context_sections.append("ДЕРЕВО ПРОЕКТА: отсутствует на сервере (при необходимости запроси через [GET_TREE]).")

    manifest_txt = format_manifest_block(file_manifest, with_urls=False)
    context_sections.append(f"ФАЙЛЫ НА СЕРВЕРЕ:\n{manifest_txt}")
    full_system_instruction = "\n\n".join(context_sections)

    cfg = get_models_config()
    raw_candidates = [
        to_api_model(cfg["direct_model"]),
        to_api_model(cfg["router_model"]),
        "models/gemini-3.5-flash-lite",
    ]
    model_candidates: list[str] = []
    for m in raw_candidates:
        if m not in model_candidates:
            model_candidates.append(m)

    total_keys = len(api_keys) if api_keys else 1
    max_key_attempts = min(total_keys, 4)
    afc_cfg = types.AutomaticFunctionCallingConfig(disable=True) if hasattr(types, "AutomaticFunctionCallingConfig") else None

    for model_name in model_candidates:
        is_25 = "2.5" in model_name
        thinking_cfg = types.ThinkingConfig(thinking_budget=0) if is_25 else types.ThinkingConfig(thinking_level="low")
        config_kwargs = {
            "system_instruction": full_system_instruction,
            "max_output_tokens": 8192,
            "thinking_config": thinking_cfg,
        }
        if afc_cfg is not None:
            config_kwargs["automatic_function_calling"] = afc_cfg
        config = types.GenerateContentConfig(**config_kwargs)

        tried: set[int] = set()
        for _ in range(max_key_attempts):
            key_idx = key_pool.get_best_key(model=model_name, exclude_indices=tried)
            tried.add(key_idx)
            client = create_genai_client(api_keys, key_idx, timeout=25000)

            flushed_any = False
            early_buf = ""
            full_parts: list[str] = []
            usage_info = {"input": 0, "cached": 0, "output": 0, "total": 0}

            try:
                if status_callback:
                    short_m = model_name.replace("models/", "")
                    await status_callback({"status": f"Direct ({short_m}, ключ #{key_idx})..."})

                stream = await asyncio.wait_for(
                    client.aio.models.generate_content_stream(
                        model=model_name,
                        contents=contents,
                        config=config,
                    ),
                    timeout=20.0
                )

                async for chunk in stream:
                    um = getattr(chunk, "usage_metadata", None)
                    if um:
                        usage_info = {
                            "input": int(getattr(um, "prompt_token_count", 0) or 0),
                            "cached": int(getattr(um, "cached_content_token_count", 0) or 0),
                            "output": int(getattr(um, "candidates_token_count", 0) or 0),
                            "total": int(getattr(um, "total_token_count", 0) or 0),
                        }

                    delta = ""
                    try:
                        delta = chunk.text or ""
                    except Exception:
                        delta = ""
                    if not delta:
                        continue

                    full_parts.append(delta)

                    if not flushed_any:
                        early_buf += delta
                        stripped_up = early_buf.lstrip().upper()
                        if stripped_up and not stripped_up.startswith("["):
                            flushed_any = True
                            if chunk_callback:
                                await chunk_callback(early_buf)
                        elif stripped_up and not _could_be_call_tag(stripped_up):
                            flushed_any = True
                            if chunk_callback:
                                await chunk_callback(early_buf)
                    else:
                        if chunk_callback:
                            await chunk_callback(delta)

                full_text = "".join(full_parts).strip()

                call_info = parse_antigravity_call(full_text)
                if call_info is not None:
                    key_pool.mark_success(key_idx, model=model_name)
                    return {
                        "escalate": True,
                        "ag_task": call_info["task"],
                        "ag_files": call_info["files"],
                        "need_network": call_info["network"],
                        "assigned_key": key_idx,
                    }

                if not flushed_any and early_buf and chunk_callback:
                    await chunk_callback(early_buf)

                key_pool.mark_success(key_idx, model=model_name)
                logger.info(f"⚡ [DIRECT {model_name}] Ключ #{key_idx} | in={usage_info['input']} out={usage_info['output']}")

                return {
                    "escalate": False,
                    "text": full_text,
                    "file_urls": {},
                    "assigned_key": key_idx,
                    "usage": usage_info,
                }

            except (asyncio.TimeoutError, TimeoutError):
                logger.warning(f"⏰ [DIRECT {model_name}] Таймаут на ключе #{key_idx}")
                if flushed_any:
                    return {
                        "escalate": False,
                        "text": "".join(full_parts).strip() + "\n\n[Поток прерван по таймауту]",
                        "file_urls": {},
                        "assigned_key": key_idx,
                        "usage": usage_info,
                    }
                key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=model_name)
            except Exception as e:
                err_str = str(e)
                err_lower = err_str.lower()
                logger.warning(f"⚠️ [DIRECT {model_name}] Ошибка на ключе #{key_idx}: {e}")
                if flushed_any:
                    return {
                        "escalate": False,
                        "text": "".join(full_parts).strip(),
                        "file_urls": {},
                        "assigned_key": key_idx,
                        "usage": usage_info,
                    }
                if _is_dead_project_error(err_str):
                    key_pool.mark_dead_all_models(key_idx)
                elif any(n in err_lower for n in ["eof", "ssl", "connection reset", "broken pipe"]):
                    await asyncio.sleep(1.0)
                elif "429" in err_lower or "quota" in err_lower or "too_many_requests" in err_lower:
                    key_pool.mark_exhausted(key_idx, duration_seconds=10800, model=model_name)
                else:
                    key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=model_name)
            finally:
                try:
                    if hasattr(client, "aio") and hasattr(client.aio, "aclose"):
                        await client.aio.aclose()
                except Exception:
                    pass

    raise Exception("Ни один ключ/модель не ответили в прямом режиме.")


run_direct_hydrogen = run_direct