# -*- coding: utf-8 -*-
import os
from typing import List

def _load_gemini_keys_from_env() -> List[str]:
    raw = os.environ.get('GEMINI_KEYS')
    if not raw:
        return []
    # Делим строку ключей по запятой и убираем лишние пробелы
    return [k.strip() for k in raw.split(',') if k.strip()]

# Экспортируем переменную для всего приложения
GEMINI_KEYS = _load_gemini_keys_from_env()