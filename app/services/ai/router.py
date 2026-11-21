# -*- coding: utf-8 -*-
import asyncio
import logging
from google.genai import types
from .key_pool import GeminiKeyPool, create_genai_client, extract_interaction_text

logger = logging.getLogger("AI_Service")

MODEL_ID = "models/gemini-3.5-flash-lite"


def format_stateless_prompt(prompt_text: str, summary: str = None, recent_history: list = None) -> str:
    if (not summary or not summary.strip()) and not recent_history:
        return prompt_text

    blocks = []
    if summary and summary.strip():
        blocks.append(f"# Краткая сводка предыдущего диалога:\n{summary.strip()}")

    if recent_history:
        lines = []
        for m in recent_history[-10:]:
            r = m.get("role")
            label = "Пользователь" if r == "user" else ("Система/IDE" if r in ("system", "tool") else "Ассистент")
            txt = (m.get("text") or "").strip()
            if not txt:
                continue
            limit = 1500 if r in ("tool", "system") else 2200
            if len(txt) > limit:
                txt = txt[:limit] + "\n...[обрезано]..."
            lines.append(f"[{label}]: {txt}")
        if lines:
            blocks.append("# Последние 10 сообщений диалога:\n" + "\n\n".join(lines))

    blocks.append(f"# Текущее сообщение:\n{prompt_text}")
    return "\n\n".join(blocks)


def build_direct_contents(prompt_text: str, recent_history: list = None) -> list[types.Content]:
    turns: list[tuple[str, str]] = []

    if recent_history:
        for m in recent_history[-10:]:
            r = m.get("role")
            txt = (m.get("text") or "").strip()
            if not txt:
                continue
            if r == "agent":
                role = "model"
                if len(txt) > 2500:
                    txt = txt[:2500] + "..."
            elif r in ("tool", "system"):
                role = "user"
                if len(txt) > 1200:
                    txt = txt[:1200] + "..."
                txt = f"[Событие IDE]: {txt}"
            else:
                role = "user"
                if len(txt) > 2500:
                    txt = txt[:2500] + "..."

            if turns and turns[-1][0] == role:
                turns[-1] = (role, turns[-1][1] + "\n\n" + txt)
            else:
                turns.append((role, txt))

    # Не начинаем с model
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


def _is_dead_project_error(err_str: str) -> bool:
    return (
        "'quota_limit_value': '0'" in err_str
        or "ApiRequestsPerMinutePerProjectPerRegion" in err_str
        or "API_KEY_INVALID" in err_str
        or "401" in err_str
    )


async def classify_route(key_pool: GeminiKeyPool, api_keys: list[str], prompt_text: str) -> str:
    if "Скачай через: curl" in prompt_text or "Ссылка: https://friday-assistant.ru/uploads/" in prompt_text:
        return "ANTIGRAVITY"

    clean_p = prompt_text.strip().lower()
    if len(clean_p) < 35 and not any(k in clean_p for k in [".", "/", "\\", "файл", "код", "сделай", "создай", "напиши", "удали", "запусти", "проект", "папк", "покажи", "посмотр", "3д", "stl", "docx"]):
        return "DIRECT"

    tried: set[int] = set()
    attempts = min(len(api_keys), 4) if api_keys else 1

    for _ in range(attempts):
        key_idx = key_pool.get_best_key(model=MODEL_ID, exclude_indices=tried)
        tried.add(key_idx)
        client = create_genai_client(api_keys, key_idx, timeout=12000)
        try:
            from .prompts import ROUTER_SYSTEM_INSTRUCTION
            interaction = await asyncio.wait_for(
                asyncio.to_thread(
                    client.interactions.create,
                    model=MODEL_ID,
                    system_instruction=ROUTER_SYSTEM_INSTRUCTION,
                    input=prompt_text[:2000],
                    generation_config={"max_output_tokens": 16, "thinking_level": "low"},
                ),
                timeout=14.0
            )
            key_pool.mark_success(key_idx, model=MODEL_ID)
            ans = extract_interaction_text(interaction).strip().upper()
            route = "ANTIGRAVITY" if "ANTIGRAVITY" in ans else "DIRECT"
            logger.info(f"🧭 [ROUTER 3.5-lite] Ключ #{key_idx} -> {route} ('{ans[:25]}')")
            return route
        except Exception as e:
            logger.warning(f"⚠️ [ROUTER] Сбой на ключе #{key_idx}: {e}")
            if _is_dead_project_error(str(e)):
                key_pool.mark_dead_all_models(key_idx)
            else:
                key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=MODEL_ID)

    return "DIRECT"


async def run_direct_hydrogen(
    key_pool: GeminiKeyPool,
    api_keys: list[str],
    prompt_text: str,
    summary: str = None,
    recent_history: list = None,
    status_callback=None,
    chunk_callback=None,
    assigned_key: int = None
):
    from .prompts import DIRECT_SYSTEM_INSTRUCTION

    contents = build_direct_contents(prompt_text, recent_history=recent_history)
    config = types.GenerateContentConfig(
        system_instruction=DIRECT_SYSTEM_INSTRUCTION,
        max_output_tokens=16384,
        thinking_config=types.ThinkingConfig(thinking_level="low"),
    )

    total_keys = len(api_keys) if api_keys else 1
    tried: set[int] = set()
    attempts = min(total_keys, 6)

    for _ in range(attempts):
        key_idx = key_pool.get_best_key(model=MODEL_ID, exclude_indices=tried)
        tried.add(key_idx)
        client = create_genai_client(api_keys, key_idx, timeout=25000)

        flushed_any = False
        early_buf = ""
        full_parts: list[str] = []
        usage_info = {"input": 0, "cached": 0, "output": 0, "total": 0}

        try:
            if status_callback:
                await status_callback({"status": f"Стрим 3.5-lite (ключ #{key_idx})..."})

            stream = await asyncio.wait_for(
                client.aio.models.generate_content_stream(
                    model=MODEL_ID,
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
                    stripped = early_buf.lstrip()
                    # Ждём до 28 символов только если текст начинается с '[' (проверка [ESCALATE_ANTIGRAVITY])
                    if stripped and not stripped.startswith("["):
                        flushed_any = True
                        if chunk_callback:
                            await chunk_callback(early_buf)
                    elif len(stripped) >= 28:
                        if "[ESCALATE_ANTIGRAVITY]" in stripped:
                            return {"escalate": True, "assigned_key": key_idx}
                        flushed_any = True
                        if chunk_callback:
                            await chunk_callback(early_buf)
                else:
                    if chunk_callback:
                        await chunk_callback(delta)

            full_text = "".join(full_parts).strip()

            if not flushed_any and "[ESCALATE_ANTIGRAVITY]" in full_text:
                return {"escalate": True, "assigned_key": key_idx}

            if not flushed_any and early_buf and chunk_callback:
                await chunk_callback(early_buf)

            key_pool.mark_success(key_idx, model=MODEL_ID)
            logger.info(f"⚡ [DIRECT STREAM 3.5-lite] Ключ #{key_idx} | Токены: in={usage_info['input']} cached={usage_info['cached']} out={usage_info['output']}")

            return {
                "escalate": False,
                "text": full_text,
                "file_urls": {},
                "assigned_key": key_idx,
                "usage": usage_info,
            }

        except (asyncio.TimeoutError, TimeoutError):
            logger.warning(f"⏰ [DIRECT STREAM] Таймаут на ключе #{key_idx}")
            if flushed_any:
                return {
                    "escalate": False,
                    "text": "".join(full_parts).strip() + "\n\n[Поток прерван по таймауту]",
                    "file_urls": {},
                    "assigned_key": key_idx,
                    "usage": usage_info,
                }
            key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=MODEL_ID)
        except Exception as e:
            err_str = str(e)
            err_lower = err_str.lower()
            logger.warning(f"⚠️ [DIRECT STREAM] Ошибка на ключе #{key_idx}: {e}")
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
                key_pool.mark_exhausted(key_idx, duration_seconds=10800, model=MODEL_ID)
            else:
                key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=MODEL_ID)
        finally:
            try:
                if hasattr(client, "aio") and hasattr(client.aio, "aclose"):
                    await client.aio.aclose()
            except Exception:
                pass

    raise Exception("Ни один ключ не ответил в прямом режиме.")


async def generate_rolling_summary(key_pool, api_keys, prev_summary, messages):
    return None