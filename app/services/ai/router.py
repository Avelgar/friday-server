# -*- coding: utf-8 -*-
import asyncio
import logging
from .key_pool import GeminiKeyPool, create_genai_client, extract_interaction_text, extract_usage_tokens
from .prompts import DIRECT_SYSTEM_INSTRUCTION, ROUTER_SYSTEM_INSTRUCTION

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
            limit = 4000 if r == "tool" else 1800
            if len(txt) > limit:
                txt = txt[:limit] + "\n...[обрезано]..."
            lines.append(f"[{label}]: {txt}")
        if lines:
            blocks.append("# Последние сообщения диалога:\n" + "\n\n".join(lines))

    blocks.append(f"# Текущее сообщение:\n{prompt_text}")
    return "\n\n".join(blocks)


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
    if len(clean_p) < 35 and not any(k in clean_p for k in [".", "/", "\\", "файл", "код", "сделай", "создай", "напиши", "удали", "запусти", "проект", "папк", "3д", "stl", "docx"]):
        return "DIRECT"

    tried: set[int] = set()
    attempts = min(len(api_keys), 4) if api_keys else 1

    for _ in range(attempts):
        key_idx = key_pool.get_best_key(model=MODEL_ID, exclude_indices=tried)
        tried.add(key_idx)
        client = create_genai_client(api_keys, key_idx, timeout=12000)
        try:
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
    assigned_key: int = None
):
    full_input = format_stateless_prompt(prompt_text, summary=summary, recent_history=recent_history)
    generation_config = {
        "max_output_tokens": 16384,
        "thinking_level": "low",
    }

    total_keys = len(api_keys) if api_keys else 1
    tried: set[int] = set()
    attempts = min(total_keys, 6)

    for _ in range(attempts):
        key_idx = key_pool.get_best_key(model=MODEL_ID, exclude_indices=tried)
        tried.add(key_idx)
        client = create_genai_client(api_keys, key_idx, timeout=25000)

        try:
            if status_callback:
                await status_callback({"status": f"Ответ 3.5-lite (ключ #{key_idx})..."})

            interaction = await asyncio.wait_for(
                asyncio.to_thread(
                    client.interactions.create,
                    model=MODEL_ID,
                    system_instruction=DIRECT_SYSTEM_INSTRUCTION,
                    input=full_input,
                    generation_config=generation_config,
                ),
                timeout=30.0
            )

            key_pool.mark_success(key_idx, model=MODEL_ID)
            output_text = extract_interaction_text(interaction).strip()
            usage = extract_usage_tokens(interaction)
            logger.info(f"⚡ [DIRECT 3.5-lite] Ключ #{key_idx} | Токены: in={usage['input']} cached={usage['cached']} out={usage['output']}")

            if "[ESCALATE_ANTIGRAVITY]" in output_text:
                return {"escalate": True, "assigned_key": key_idx}

            return {
                "escalate": False,
                "text": output_text,
                "file_urls": {},
                "assigned_key": key_idx,
                "usage": usage,
            }
        except (asyncio.TimeoutError, TimeoutError):
            logger.warning(f"⏰ [DIRECT 3.5-lite] Таймаут на ключе #{key_idx}")
            key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=MODEL_ID)
        except Exception as e:
            err_str = str(e)
            err_lower = err_str.lower()
            logger.warning(f"⚠️ [DIRECT 3.5-lite] Ошибка на ключе #{key_idx}: {e}")
            if _is_dead_project_error(err_str):
                key_pool.mark_dead_all_models(key_idx)
            elif any(n in err_lower for n in ["eof", "ssl", "connection reset", "broken pipe"]):
                await asyncio.sleep(1.0)
            elif "429" in err_lower or "quota" in err_lower or "too_many_requests" in err_lower:
                key_pool.mark_exhausted(key_idx, duration_seconds=10800, model=MODEL_ID)
            else:
                key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=MODEL_ID)

    raise Exception("Ни один ключ не ответил в прямом режиме.")


async def generate_rolling_summary(key_pool, api_keys, prev_summary, messages):
    return None