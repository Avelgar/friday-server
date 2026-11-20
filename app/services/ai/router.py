# -*- coding: utf-8 -*-
import asyncio
import logging
from .key_pool import GeminiKeyPool, create_genai_client, extract_interaction_text, extract_usage_tokens
from .prompts import DIRECT_SYSTEM_INSTRUCTION, ROUTER_SYSTEM_INSTRUCTION, SUMMARY_SYSTEM_INSTRUCTION

logger = logging.getLogger("AI_Service")


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


async def classify_route(key_pool: GeminiKeyPool, api_keys: list[str], prompt_text: str) -> str:
    if "Скачай через: curl" in prompt_text or "Ссылка: https://friday-assistant.ru/uploads/" in prompt_text:
        return "ANTIGRAVITY"

    clean_p = prompt_text.strip().lower()
    if len(clean_p) < 35 and not any(k in clean_p for k in [".", "/", "\\", "файл", "код", "сделай", "создай", "напиши", "удали", "запусти", "проект", "папк", "3д", "stl", "docx"]):
        return "DIRECT"

    model = "models/gemini-3.5-flash-lite"
    tried: set[int] = set()
    attempts = min(len(api_keys), 4) if api_keys else 1

    for _ in range(attempts):
        key_idx = key_pool.get_best_key(model=model, exclude_indices=tried)
        tried.add(key_idx)
        client = create_genai_client(api_keys, key_idx, timeout=12000)
        try:
            interaction = await asyncio.wait_for(
                asyncio.to_thread(
                    client.interactions.create,
                    model=model,
                    system_instruction=ROUTER_SYSTEM_INSTRUCTION,
                    input=prompt_text[:2000],
                    generation_config={"max_output_tokens": 16, "thinking_level": "low"},
                ),
                timeout=14.0
            )
            key_pool.mark_success(key_idx, model=model)
            ans = extract_interaction_text(interaction).strip().upper()
            route = "ANTIGRAVITY" if "ANTIGRAVITY" in ans else "DIRECT"
            logger.info(f"🧭 [ROUTER 3.5-lite] Ключ #{key_idx} -> {route} ('{ans[:25]}')")
            return route
        except Exception as e:
            logger.warning(f"⚠️ [ROUTER] Сбой на ключе #{key_idx}: {e}")
            key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=model)

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
    full_input = format_stateless_prompt(prompt_text, summary=summary, recent_history=recent_history)
    has_url = ("http://" in prompt_text or "https://" in prompt_text)
    tools = [{"type": "url_context"}] if has_url else None

    total_keys = len(api_keys) if api_keys else 1

    for model_name in ("models/gemini-3.5-flash-lite"):
        tried: set[int] = set()
        attempts = 2 if "3.6" in model_name else min(total_keys, 4)
        req_timeout_ms = 12000 if "3.6" in model_name else 25000
        wait_sec = 14.0 if "3.6" in model_name else 30.0
        thinking = "low" if "lite" in model_name or len(prompt_text.strip()) < 60 else "medium"
        generation_config = {
            "max_output_tokens": 16384,
            "thinking_level": thinking,
        }

        for _ in range(attempts):
            key_idx = key_pool.get_best_key(model=model_name, exclude_indices=tried)
            tried.add(key_idx)
            client = create_genai_client(api_keys, key_idx, timeout=25000)

            try:
                if status_callback:
                    short_m = "3.6 Flash" if "3.6" in model_name else "3.5 Lite"
                    await status_callback({"status": f"Быстрый ответ {short_m} (ключ #{key_idx})..."})

                create_kw = {
                    "model": model_name,
                    "system_instruction": DIRECT_SYSTEM_INSTRUCTION,
                    "input": full_input,
                    "generation_config": generation_config,
                }
                if tools:
                    create_kw["tools"] = tools

                interaction = await asyncio.wait_for(
                    asyncio.to_thread(client.interactions.create, **create_kw),
                    timeout=30.0
                )

                key_pool.mark_success(key_idx, model=model_name)
                output_text = extract_interaction_text(interaction).strip()
                usage = extract_usage_tokens(interaction)
                logger.info(f"⚡ [DIRECT {model_name}] Ключ #{key_idx} | Токены: in={usage['input']} cached={usage['cached']} out={usage['output']}")

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
                logger.warning(f"⏰ [DIRECT {model_name}] Таймаут на ключе #{key_idx}")
                key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=model_name)
            except Exception as e:
                err_lower = str(e).lower()
                logger.warning(f"⚠️ [DIRECT {model_name}] Ошибка на ключе #{key_idx}: {e}")
                if any(n in err_lower for n in ["eof", "ssl", "connection reset", "broken pipe"]):
                    await asyncio.sleep(1.0)
                    continue
                if "429" in err_lower or "quota" in err_lower or "too_many_requests" in err_lower:
                    key_pool.mark_exhausted(key_idx, duration_seconds=10800, model=model_name)
                else:
                    key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=model_name)

    raise Exception("Ни одна из прямых моделей не ответила по доступным ключам.")


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

    model = "models/gemini-3.5-flash-lite"
    tried: set[int] = set()

    for _ in range(3):
        key_idx = key_pool.get_best_key(model=model, exclude_indices=tried)
        tried.add(key_idx)
        client = create_genai_client(api_keys, key_idx, timeout=30000)
        try:
            interaction = await asyncio.wait_for(
                asyncio.to_thread(
                    client.interactions.create,
                    model=model,
                    system_instruction=SUMMARY_SYSTEM_INSTRUCTION,
                    input=prompt,
                    generation_config={"max_output_tokens": 2048, "thinking_level": "low"},
                ),
                timeout=35.0
            )
            key_pool.mark_success(key_idx, model=model)
            res = extract_interaction_text(interaction).strip()
            if res:
                logger.info(f"📝 [SUMMARY UPDATED] Ключ #{key_idx} | Длина сводки: {len(res)} симв.")
                return res
        except Exception as e:
            logger.warning(f"⚠️ [SUMMARY ERROR] Ключ #{key_idx}: {e}")
            key_pool.mark_exhausted(key_idx, duration_seconds=1800, model=model)

    return None