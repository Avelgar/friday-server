# -*- coding: utf-8 -*-
import os
import json
import time
import random
import logging
import threading
from datetime import datetime
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


def _norm_model(model: str) -> str:
    m = (model or "antigravity").strip().lower()
    if m.startswith("models/"):
        m = m[len("models/"):]
    return m


class GeminiKeyPool:
    def __init__(self, keys: list[str], state_file: str = "/opt/friday/storage/key_pool_state.json"):
        self.keys = keys or []
        self._state_file = state_file
        self._lock = threading.Lock()
        # state[model][str(idx)] = {"exhausted_until": float, "used_today": int, "errors_today": int, "last_used": float}
        self._state = {"date": self._today(), "models": {}}
        self._load_state()

    def _today(self) -> str:
        return datetime.utcnow().strftime("%Y-%m-%d")

    def _load_state(self):
        try:
            if os.path.exists(self._state_file):
                with open(self._state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and isinstance(data.get("models"), dict):
                    self._state = data
                    self._ensure_today_locked()
        except Exception as e:
            logger.warning(f"⚠️ [Key Pool] Не удалось прочитать state: {e}")

    def _save_state_locked(self):
        try:
            os.makedirs(os.path.dirname(self._state_file), exist_ok=True)
            tmp = self._state_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._state, f, ensure_ascii=False)
            os.replace(tmp, self._state_file)
        except Exception:
            pass

    def _ensure_today_locked(self):
        today = self._today()
        if self._state.get("date") != today:
            self._state = {"date": today, "models": {}}

    def _entry_locked(self, model: str, key_index: int) -> dict:
        self._ensure_today_locked()
        m = _norm_model(model)
        models_map = self._state.setdefault("models", {})
        m_map = models_map.setdefault(m, {})
        k_str = str(key_index)
        if k_str not in m_map:
            m_map[k_str] = {
                "exhausted_until": 0.0,
                "used_today": 0,
                "errors_today": 0,
                "last_used": 0.0,
            }
        return m_map[k_str]

    def mark_exhausted(self, key_index: int, duration_seconds: float = 3600, model: str = "antigravity"):
        if not self.keys:
            return
        with self._lock:
            entry = self._entry_locked(model, key_index)
            entry["exhausted_until"] = time.time() + duration_seconds
            entry["errors_today"] = int(entry.get("errors_today", 0)) + 1
            self._save_state_locked()
        logger.warning(f"🚫 [Key Pool] ({_norm_model(model)}) Ключ #{key_index} в паузе на {duration_seconds/60:.0f} мин.")

    def mark_success(self, key_index: int, model: str = "antigravity"):
        if not self.keys:
            return
        with self._lock:
            entry = self._entry_locked(model, key_index)
            entry["exhausted_until"] = 0.0
            self._save_state_locked()

    def is_usable(self, key_index: int, model: str = "antigravity") -> bool:
        if not self.keys:
            return True
        if key_index < 0 or key_index >= len(self.keys):
            return False
        with self._lock:
            entry = self._entry_locked(model, key_index)
            until = float(entry.get("exhausted_until", 0.0))
            if until <= 0:
                return True
            if time.time() >= until:
                entry["exhausted_until"] = 0.0
                self._save_state_locked()
                return True
            return False

    def get_best_key(self, model: str = "antigravity", exclude_indices: set[int] = None) -> int:
        """Выбирает самый свежий/неиспользованный живой ключ для конкретной модели."""
        total = len(self.keys)
        if total <= 1:
            return 0

        excluded = exclude_indices or set()
        now = time.time()

        with self._lock:
            self._ensure_today_locked()
            candidates = [i for i in range(total) if i not in excluded]
            if not candidates:
                candidates = list(range(total))

            usable = []
            for idx in candidates:
                entry = self._entry_locked(model, idx)
                until = float(entry.get("exhausted_until", 0.0))
                if until > now:
                    continue
                if until > 0:
                    entry["exhausted_until"] = 0.0
                usable.append((
                    int(entry.get("errors_today", 0)),
                    int(entry.get("used_today", 0)),
                    float(entry.get("last_used", 0.0)),
                    random.random(),
                    idx
                ))

            if usable:
                usable.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
                chosen = usable[0][4]
            else:
                # Все кандидаты в бане — берём тот, у которого бан заканчивается раньше всех
                chosen = min(
                    candidates,
                    key=lambda idx: float(self._entry_locked(model, idx).get("exhausted_until", 0.0))
                )
                self._entry_locked(model, chosen)["exhausted_until"] = 0.0

            chosen_entry = self._entry_locked(model, chosen)
            chosen_entry["used_today"] = int(chosen_entry.get("used_today", 0)) + 1
            chosen_entry["last_used"] = now
            self._save_state_locked()
            return chosen

    def get_next_key(self, exclude_index: int = None, model: str = "antigravity") -> int:
        excl = {exclude_index} if exclude_index is not None else None
        return self.get_best_key(model=model, exclude_indices=excl)

    def get_available_key(self, exclude_index: int = None, model: str = "antigravity") -> int:
        return self.get_next_key(exclude_index=exclude_index, model=model)


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