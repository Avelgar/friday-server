# -*- coding: utf-8 -*-
import asyncio
import logging
from .key_pool import GeminiKeyPool, create_genai_client, extract_interaction_text, extract_usage_tokens
from .prompts import DIRECT_SYSTEM_INSTRUCTION, ROUTER_SYSTEM_INSTRUCTION, SUMMARY_SYSTEM_INSTRUCTION

logger = logging.getLogger("AI_Service")


def format_stateless_prompt(prompt_text: str, summary: str = None, recent_history: list = None) -> str:
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


async def classify_route(key_pool: GeminiKeyPool, api_keys: list[str], prompt_text: str) -> str:
    # Быстрая эвристика без сетевого вызова для вложений с curl и явных файлов
    if "Скачай через: curl" in prompt_text or "Ссылка: https://friday-assistant.ru/uploads/" in prompt_text:
        return "ANTIGRAVITY"

    total_keys = len(api_keys)
    attempts = min(total_keys, 3) if total_keys > 0 else 1
    key_idx = key_pool.get_next_key()

    for _ in range(attempts):
        client = create_genai_client(api_keys, key_idx, timeout=12000)
        try:
            interaction = await asyncio.wait_for(
                asyncio.to_thread(
                    client.interactions.create,
                    model="models/gemini-3.5-flash-lite",
                    system_instruction=ROUTER_SYSTEM_INSTRUCTION,
                    input=prompt_text[:2000],
                    generation_config={"max_output_tokens": 16, "thinking_level": "low"},
                ),
                timeout=14.0
            )
            ans = extract_interaction_text(interaction).strip().upper()
            route = "ANTIGRAVITY" if "ANTIGRAVITY" in ans else "DIRECT"
            logger.info(f"🧭 [ROUTER 3.5-lite] Ключ #{key_idx} -> {route} ('{ans[:25]}')")
            return route
        except Exception as e:
            logger.warning(f"⚠️ [ROUTER] Сбой на ключе #{key_idx}: {e}")
            key_pool.mark_exhausted(key_idx, duration_seconds=600)
            key_idx = key_pool.get_next_key(exclude_index=key_idx)

    return "ANTIGRAVITY"


async def run_direct_hydrogen(
    key_pool: GeminiKeyPool,
    api_keys: list[str],
    prompt_text: str,
    summary: str = None,
    recent_history: list = None,
    status_callback=None,
    assigned_key: int = None
):
    total_keys = len(api_keys)
    if assigned_key is None or not key_pool.is_usable(assigned_key):
        assigned_key = key_pool.get_next_key(exclude_index=assigned_key)

    max_attempts = min(total_keys, 8) if total_keys > 0 else 2
    full_input = format_stateless_prompt(prompt_text, summary=summary, recent_history=recent_history)

    tools = [
        {"type": "google_search"},
        {"type": "url_context"},
    ]
    generation_config = {
        "max_output_tokens": 16384,
        "thinking_level": "medium",
    }

    for _ in range(max_attempts):
        client = create_genai_client(api_keys, assigned_key, timeout=60000)
        try:
            if status_callback:
                await status_callback({"status": f"Прямой ответ Hydrogen (ключ #{assigned_key})..."})

            interaction = await asyncio.wait_for(
                asyncio.to_thread(
                    client.interactions.create,
                    model="models/gemini-3.6-flash",
                    system_instruction=DIRECT_SYSTEM_INSTRUCTION,
                    input=full_input,
                    tools=tools,
                    generation_config=generation_config,
                ),
                timeout=65.0
            )

            output_text = extract_interaction_text(interaction).strip()
            usage = extract_usage_tokens(interaction)
            logger.info(f"⚡ [DIRECT HYDROGEN] Ключ #{assigned_key} | Токены: in={usage['input']} cached={usage['cached']} out={usage['output']}")

            if "[ESCALATE_ANTIGRAVITY]" in output_text:
                return {"escalate": True, "assigned_key": assigned_key}

            return {
                "escalate": False,
                "text": output_text,
                "file_urls": {},
                "assigned_key": assigned_key,
                "usage": usage,
            }
        except (asyncio.TimeoutError, TimeoutError):
            logger.warning(f"⏰ [DIRECT HYDROGEN] Таймаут на ключе #{assigned_key}")
            key_pool.mark_exhausted(assigned_key, duration_seconds=1800)
            assigned_key = key_pool.get_next_key(exclude_index=assigned_key)
        except Exception as e:
            err_lower = str(e).lower()
            logger.warning(f"⚠️ [DIRECT HYDROGEN] Ошибка на ключе #{assigned_key}: {e}")
            if any(n in err_lower for n in ["eof", "ssl", "connection reset", "broken pipe"]):
                await asyncio.sleep(1.5)
                continue
            key_pool.mark_exhausted(assigned_key, duration_seconds=3600)
            assigned_key = key_pool.get_next_key(exclude_index=assigned_key)

    raise Exception("Прямой Hydrogen не ответил ни на одном ключе.")


async def generate_rolling_summary(
    key_pool: GeminiKeyPool,
    api_keys: list[str],
    prev_summary: str,
    messages: list
) -> str | None:
    if not messages:
        return prev_summary

    lines = []
    for m in messages:
        r = m.get("role")
        label = "Пользователь" if r == "user" else ("Система/IDE" if r in ("system", "tool") else "Ассистент")
        txt = (m.get("text") or "").strip()
        if txt:
            if len(txt) > 1200:
                txt = txt[:1200] + "..."
            lines.append(f"[{label}]: {txt}")

    if not lines:
        return prev_summary

    prompt = ""
    if prev_summary and prev_summary.strip():
        prompt += f"Предыдущая сводка:\n{prev_summary.strip()}\n\n"
    prompt += "Новые сообщения для включения в сводку:\n" + "\n".join(lines)

    key_idx = key_pool.get_next_key()
    for _ in range(2):
        client = create_genai_client(api_keys, key_idx, timeout=30000)
        try:
            interaction = await asyncio.wait_for(
                asyncio.to_thread(
                    client.interactions.create,
                    model="models/gemini-4-hydrogen",
                    system_instruction=SUMMARY_SYSTEM_INSTRUCTION,
                    input=prompt,
                    generation_config={"max_output_tokens": 2048, "thinking_level": "low"},
                ),
                timeout=35.0
            )
            res = extract_interaction_text(interaction).strip()
            if res:
                logger.info(f"📝 [SUMMARY UPDATED] Длина сводки: {len(res)} симв.")
                return res
        except Exception as e:
            logger.warning(f"⚠️ [SUMMARY ERROR] Ключ #{key_idx}: {e}")
            key_idx = key_pool.get_next_key(exclude_index=key_idx)

    return None
