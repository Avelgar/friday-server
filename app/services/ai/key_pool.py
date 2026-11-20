# -*- coding: utf-8 -*-
import os
import time
import logging
from google import genai
from google.genai import types

logger = logging.getLogger("AI_Service")

try:
    from app.config.secrets import GEMINI_KEYS
except ImportError:
    GEMINI_KEYS = []

try:
    from app.config.settings import PROXY_URL
except ImportError:
    PROXY_URL = os.environ.get('PROXY_URL') or os.environ.get('HTTPS_PROXY') or os.environ.get('HTTP_PROXY') or ''

if PROXY_URL:
    logger.info(f"🌐 [AI_Service] Используется PROXY: {PROXY_URL.split('@')[-1] if '@' in PROXY_URL else PROXY_URL}")
else:
    logger.info("🌐 [AI_Service] Прямое подключение без прокси (PROXY_URL не задан)")


class GeminiKeyPool:
    def __init__(self, keys: list[str]):
        self.keys = keys or []
        self.exhausted_keys = {}
        self._round_robin_counter = 0

    def mark_exhausted(self, key_index: int, duration_seconds: float = 3600):
        now = time.time()
        self.exhausted_keys[key_index] = now + duration_seconds
        logger.warning(f"🚫 [Key Pool] Ключ #{key_index} помечен как НЕРАБОЧИЙ на {duration_seconds/60:.0f} мин.")

    def is_usable(self, key_index: int) -> bool:
        if not self.keys:
            return True
        if key_index not in self.exhausted_keys:
            return True
        if time.time() > self.exhausted_keys[key_index]:
            del self.exhausted_keys[key_index]
            return True
        return False

    def get_next_key(self, exclude_index: int = None) -> int:
        total = len(self.keys)
        if total <= 1:
            return 0

        for _ in range(total):
            self._round_robin_counter = (self._round_robin_counter + 1) % total
            candidate = self._round_robin_counter
            if exclude_index is not None and candidate == exclude_index:
                continue
            if self.is_usable(candidate):
                return candidate

        if self.exhausted_keys:
            earliest = min(self.exhausted_keys.keys(), key=lambda k: self.exhausted_keys[k])
            del self.exhausted_keys[earliest]
            return earliest
        return 0

    def get_available_key(self, exclude_index: int = None) -> int:
        return self.get_next_key(exclude_index=exclude_index)

def build_http_options(timeout=60000):
    client_args = {}
    async_client_args = {}
    if PROXY_URL:
        client_args["proxy"] = PROXY_URL
        async_client_args["proxy"] = PROXY_URL

    kwargs = {
        "api_version": "v1beta",
        "timeout": timeout,
        "client_args": client_args or None,
        "async_client_args": async_client_args or None,
    }
    if hasattr(types, "HttpRetryOptions"):
        kwargs["retry_options"] = types.HttpRetryOptions(attempts=1)

    return types.HttpOptions(**kwargs)


def create_genai_client(api_keys: list[str], key_index: int = 0, timeout=180000):
    api_key = api_keys[key_index] if api_keys and key_index < len(api_keys) else None
    return genai.Client(
        http_options=build_http_options(timeout=timeout),
        api_key=api_key
    )


def create_live_client(api_keys: list[str]):
    live_key = api_keys[0] if api_keys else None
    return genai.Client(
        http_options=build_http_options(timeout=None),
        api_key=live_key
    )


def extract_interaction_text(interaction) -> str:
    txt = getattr(interaction, "output_text", None)
    if txt:
        return txt
    steps = getattr(interaction, "steps", None) or []
    for step in reversed(steps):
        content = getattr(step, "content", None) or []
        parts = []
        for item in content:
            t = getattr(item, "text", None)
            if t:
                parts.append(t)
        if parts:
            return "\n".join(parts)
    return ""


def extract_usage_tokens(interaction) -> dict:
    usage = getattr(interaction, "usage", None)
    if not usage:
        return {"input": 0, "cached": 0, "output": 0, "total": 0}
    return {
        "input": int(getattr(usage, "total_input_tokens", 0) or 0),
        "cached": int(getattr(usage, "total_cached_tokens", 0) or 0),
        "output": int(getattr(usage, "total_output_tokens", 0) or 0),
        "total": int(getattr(usage, "total_tokens", 0) or 0),
    }
