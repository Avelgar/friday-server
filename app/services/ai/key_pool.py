# -*- coding: utf-8 -*-
import os
import re
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


# Стратегия выбора ключа на каждый запрос:
#   freest      — самый свободный (меньше ошибок, меньше использований, дольше не использовался);
#   round_robin — строго по кругу.
KEY_STRATEGY = (os.environ.get("KEY_STRATEGY") or "freest").strip().lower()

# Сколько держать ключ в паузе после ошибки (секунды)
INVALID_KEY_COOLDOWN = float(os.environ.get("KEY_INVALID_COOLDOWN", 86400))  # ключ/проект невалиден
RATE_LIMIT_COOLDOWN = float(os.environ.get("KEY_RATE_COOLDOWN", 900))        # 429 / квота на минуту


def mask_key(api_key: str | None) -> str:
    """Безопасный отпечаток ключа для логов: только последние 4 символа."""
    s = str(api_key or "").strip()
    if not s:
        return "<нет ключа>"
    return "…" + s[-4:] if len(s) > 8 else "…" + s[-2:]


def format_key_label(api_keys: list[str], index: int) -> str:
    """Единый формат метки ключа в логах: #3 (№4 из 8) …a1b2."""
    if not api_keys:
        return f"#{index} (№{int(index) + 1} из 0) <нет ключа>"
    if 0 <= int(index) < len(api_keys):
        return f"#{index} (№{int(index) + 1} из {len(api_keys)}) {mask_key(api_keys[index])}"
    return f"#{index} (№{int(index) + 1} из {len(api_keys)}) <индекс вне пула>"


def log_keys_loaded(keys: list[str]) -> None:
    """Пишет в лог номера всех ключей и их последние 4 символа при старте пула."""
    if not keys:
        logger.error("🔑 [Key Pool] GEMINI_KEYS пуст — запросы к Gemini работать не будут.")
        return
    parts = [f"#{i} {mask_key(k)}" for i, k in enumerate(keys)]
    logger.info(f"🔑 [Key Pool] Загружено ключей: {len(keys)} | " + ", ".join(parts))


def _extract_status_code(exc) -> int | None:
    """Достаёт HTTP-код из исключения google.genai (APIError.code / .status / .response)."""
    for attr in ("status_code", "code", "status"):
        val = getattr(exc, attr, None)
        if isinstance(val, bool):
            continue
        if isinstance(val, int) and 100 <= val <= 599:
            return val
        if isinstance(val, str) and val.strip().isdigit() and 100 <= int(val.strip()) <= 599:
            return int(val.strip())

    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None)
    if isinstance(code, int) and 100 <= code <= 599:
        return code

    m = re.search(r'"code"\s*:\s*(\d{3})\b', str(exc or ""))
    if m:
        return int(m.group(1))
    return None


def classify_key_error(exc) -> str | None:
    """
    Классифицирует ошибку ключа по HTTP-коду/официальным маркерам, а НЕ по подстроке "401"
    (раньше ошибка с "1401" в тексте банила живой ключ на сутки).

    "invalid_key" — ключ/проект бесполезен: 401/403, API_KEY_INVALID, нулевая квота проекта;
    "rate_limit"  — временный лимит: 429/RESOURCE_EXHAUSTED, квота на минуту;
    None          — обычная ошибка (сеть, 5xx, кривой ответ модели), ключ трогать не нужно.
    """
    text = str(exc or "")
    low = text.lower()

    if "'quota_limit_value': '0'" in text:
        return "invalid_key"
    if "api_key_invalid" in low or "api key not valid" in low or "unauthenticated" in low:
        return "invalid_key"

    status = _extract_status_code(exc)
    if status in (401, 403):
        return "invalid_key"
    if status == 429:
        return "rate_limit"

    if "permission_denied" in low and "quota" not in low:
        return "invalid_key"
    if "resource_exhausted" in low or "rate limit" in low or "too many requests" in low:
        return "rate_limit"
    if "quota" in low or "apirequestsperminuteperprojectperregion" in low:
        return "rate_limit"
    return None



class GeminiKeyPool:
    def __init__(self, keys: list[str], state_file: str = "/opt/friday/storage/key_pool_state.json"):
        self.keys = keys or []
        self._state_file = state_file
        self._lock = threading.Lock()
        # state[model][str(idx)] = {"exhausted_until": float, "used_today": int, "errors_today": int, "last_used": float}
        self._state = {"date": self._today(), "models": {}}
        # курсор круговой ротации (используется при KEY_STRATEGY=round_robin)
        self._rr: dict[str, int] = {}
        self._load_state()
        log_keys_loaded(self.keys)

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

    def pick_key_for_request(self, model: str = "antigravity", avoid_index: int | None = None) -> int:
        """
        Ключ для НОВОГО запроса: каждый запрос заново выбирает самый свободный ключ.

        freest (по умолчанию) — get_freest_key(): нагрузка считается по всем моделям сразу;
        round_robin — строго по кругу (KEY_STRATEGY=round_robin в окружении).
        """
        total = len(self.keys)
        if total <= 1:
            return 0
        if KEY_STRATEGY == "round_robin":
            return self.next_rotating_key(model=model, avoid_index=avoid_index)
        return self.get_freest_key(model=model)

    def get_freest_key(self, model: str = "antigravity", exclude_indices: set[int] = None) -> int:
        """
        Самый свободный ключ на текущий момент.

        Нагрузка (ошибки / использований за сутки / когда использовался) считается
        по ВСЕМ моделям сразу, иначе Direct, Планировщик и Antigravity живут в разных
        счётчиках и получают один и тот же «нулевой» ключ.
        Пауза (exhausted_until) учитывается только для запрошенной модели.
        """
        total = len(self.keys)
        if total <= 1:
            return 0

        excluded = exclude_indices or set()
        m = _norm_model(model)
        now = time.time()

        with self._lock:
            self._ensure_today_locked()
            candidates = [i for i in range(total) if i not in excluded]
            if not candidates:
                candidates = list(range(total))

            models_map = self._state.setdefault("models", {})
            buckets = list(models_map.values())   # снимок: _entry_locked может добавить запись
            scored = []
            for idx in candidates:
                entry_m = self._entry_locked(m, idx)
                until = float(entry_m.get("exhausted_until", 0.0))
                if until > now:
                    continue                      # ключ в паузе для этой модели
                if until > 0:
                    entry_m["exhausted_until"] = 0.0

                used_total = 0
                errors_total = 0
                last_used = 0.0
                for per_key in buckets:
                    e = per_key.get(str(idx)) if isinstance(per_key, dict) else None
                    if not isinstance(e, dict):
                        continue
                    used_total += int(e.get("used_today", 0) or 0)
                    errors_total += int(e.get("errors_today", 0) or 0)
                    last_used = max(last_used, float(e.get("last_used", 0.0) or 0.0))
                scored.append((errors_total, used_total, last_used, random.random(), idx))

            if scored:
                scored.sort(key=lambda x: x[:4])
                chosen = scored[0][4]
            else:
                # все кандидаты в паузе — берём того, у кого она кончается раньше всех
                chosen = min(candidates, key=lambda i: float(self._entry_locked(m, i).get("exhausted_until", 0.0)))
                self._entry_locked(m, chosen)["exhausted_until"] = 0.0

            entry = self._entry_locked(m, chosen)
            entry["used_today"] = int(entry.get("used_today", 0)) + 1
            entry["last_used"] = now
            self._save_state_locked()
            return chosen

    def next_rotating_key(self, model: str = "antigravity", avoid_index: int | None = None) -> int:
        """Строгая ротация по кругу: каждый следующий вызов отдаёт следующий живой ключ."""
        total = len(self.keys)
        if total <= 1:
            return 0

        m = _norm_model(model)
        now = time.time()

        with self._lock:
            start = int(self._rr.get(m, 0)) % total
            order = [(start + step) % total for step in range(total)]

            usable: list[int] = []
            for idx in order:
                entry = self._entry_locked(m, idx)
                until = float(entry.get("exhausted_until", 0.0))
                if until > now:
                    continue
                if until > 0:
                    entry["exhausted_until"] = 0.0
                usable.append(idx)

            # не отдавать тот же ключ, что был выдан предыдущим шагом этого же запроса
            if avoid_index is not None and len(usable) > 1 and usable[0] == avoid_index:
                usable.append(usable.pop(0))

            if usable:
                chosen = usable[0]
            else:
                # все в паузе — берём того, у кого пауза кончается раньше всех
                chosen = min(order, key=lambda i: float(self._entry_locked(m, i).get("exhausted_until", 0.0)))
                self._entry_locked(m, chosen)["exhausted_until"] = 0.0

            self._rr[m] = (chosen + 1) % total
            entry = self._entry_locked(m, chosen)
            entry["used_today"] = int(entry.get("used_today", 0)) + 1
            entry["last_used"] = now
            self._save_state_locked()

        logger.info(f"🔁 [Key Pool] Ротация ({m}) -> {format_key_label(self.keys, chosen)}")
        return chosen

    def register_key_error(
        self,
        key_index: int,
        model: str = "antigravity",
        exc=None,
        invalid_cooldown: float = None,
        rate_cooldown: float = None,
    ) -> str | None:
        """
        Единая реакция на ошибку ключа.
        invalid_key (401/403/API_KEY_INVALID) — пауза по всем моделям (24ч по умолчанию);
        rate_limit (429/квота)             — пауза только для этой модели (15 мин по умолчанию).
        Возвращает тип ошибки или None, если ключ трогать не нужно.
        """
        kind = classify_key_error(exc)
        if kind == "invalid_key":
            self.mark_dead_all_models(
                key_index,
                duration_seconds=INVALID_KEY_COOLDOWN if invalid_cooldown is None else invalid_cooldown,
            )
        elif kind == "rate_limit":
            self.mark_exhausted(
                key_index,
                duration_seconds=RATE_LIMIT_COOLDOWN if rate_cooldown is None else rate_cooldown,
                model=model,
            )
        if kind:
            logger.warning(
                f"⚠️ [Key Pool] Ошибка ключа {format_key_label(self.keys, key_index)} "
                f"({_norm_model(model)}) -> {kind}"
            )
        return kind

    def get_available_key(self, exclude_index: int = None, model: str = "antigravity") -> int:
        return self.get_next_key(exclude_index=exclude_index, model=model)

    def mark_dead_all_models(self, key_index: int, duration_seconds: float = 86400):
        if not self.keys:
            return
        with self._lock:
            until = time.time() + duration_seconds
            models_map = self._state.setdefault("models", {})
            all_models = set(models_map.keys()) | {"antigravity", "gemini-3.5-flash-lite"}
            for m in all_models:
                entry = self._entry_locked(m, key_index)
                entry["exhausted_until"] = until
                entry["errors_today"] = int(entry.get("errors_today", 0)) + 10
            self._save_state_locked()
        logger.error(
            f"☠️ [Key Pool] Ключ #{key_index} заблокирован для ВСЕХ моделей "
            f"на {duration_seconds / 3600:.1f} ч."
        )


def build_http_options(timeout=60000, with_retry_opt: bool = True):
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
    if with_retry_opt and hasattr(types, "HttpRetryOptions"):
        kwargs["retry_options"] = types.HttpRetryOptions(attempts=1)

    return types.HttpOptions(**kwargs)
        
def create_genai_client(api_keys: list[str], key_index: int = 0, timeout=180000, with_retry_opt: bool = True):
    """Единственная точка, где ключ «ставится» в клиент — здесь же он пишется в лог."""
    if not api_keys:
        raise RuntimeError("GEMINI_KEYS пуст: не задан ни один API-ключ Gemini")

    try:
        idx = int(key_index)
    except (TypeError, ValueError):
        idx = 0

    if idx < 0 or idx >= len(api_keys):
        logger.error(
            f"🔑 [KEY BIND] Некорректный индекс ключа: запрошен #{key_index}, "
            f"в пуле {len(api_keys)} ключ(ей) — откатываюсь на #0"
        )
        idx = 0

    logger.info(f"🔑 [KEY BIND] {format_key_label(api_keys, idx)}")

    return genai.Client(
        http_options=build_http_options(timeout=timeout, with_retry_opt=with_retry_opt),
        api_key=api_keys[idx]
    )


def create_live_client(api_keys: list[str], key_index: int = 0):
    """Голос/ Live: по-прежнему ключ #0 по умолчанию, но теперь с логом ключа."""
    if not api_keys:
        raise RuntimeError("GEMINI_KEYS пуст: не задан ни один API-ключ Gemini")

    idx = int(key_index) if isinstance(key_index, int) and 0 <= int(key_index) < len(api_keys) else 0
    logger.info(f"🎙️ [LIVE KEY BIND] {format_key_label(api_keys, idx)}")

    return genai.Client(
        http_options=build_http_options(timeout=None),
        api_key=api_keys[idx]
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