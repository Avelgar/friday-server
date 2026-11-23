# -*- coding: utf-8 -*-
import os
import json
import html
import time
import threading
from datetime import datetime

CONFIG_PATH = "/opt/friday/storage/ai_models.json"

DEFAULT_MODELS = {
    "antigravity_model": "gemini-3.5-flash-lite",
    "direct_model": "gemini-3.5-flash-lite",
    "router_model": "gemini-3.5-flash-lite",
}

_lock = threading.Lock()
_cached_mtime = -1.0
_cached_config = dict(DEFAULT_MODELS)


def normalize_model_name(name: str, default: str = "gemini-3.5-flash-lite") -> str:
    val = str(name or "").strip()
    if not val:
        return default
    if val.startswith("models/"):
        val = val[len("models/"):]
    return val


def to_api_model(name: str, default: str = "gemini-3.5-flash-lite") -> str:
    return f"models/{normalize_model_name(name, default)}"


def get_models_config() -> dict:
    global _cached_mtime, _cached_config
    with _lock:
        try:
            if os.path.exists(CONFIG_PATH):
                mtime = os.path.getmtime(CONFIG_PATH)
                if mtime != _cached_mtime:
                    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                        raw = json.load(f)
                    if isinstance(raw, dict):
                        _cached_config = {
                            "antigravity_model": normalize_model_name(raw.get("antigravity_model"), DEFAULT_MODELS["antigravity_model"]),
                            "direct_model": normalize_model_name(raw.get("direct_model"), DEFAULT_MODELS["direct_model"]),
                            "router_model": normalize_model_name(raw.get("router_model"), DEFAULT_MODELS["router_model"]),
                            "updated_at": raw.get("updated_at", ""),
                        }
                        _cached_mtime = mtime
        except Exception:
            pass
        return dict(_cached_config)


def save_models_config(data: dict) -> dict:
    global _cached_mtime, _cached_config
    new_cfg = {
        "antigravity_model": normalize_model_name(data.get("antigravity_model"), DEFAULT_MODELS["antigravity_model"]),
        "direct_model": normalize_model_name(data.get("direct_model"), DEFAULT_MODELS["direct_model"]),
        "router_model": normalize_model_name(data.get("router_model"), DEFAULT_MODELS["router_model"]),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with _lock:
        os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
        tmp = CONFIG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(new_cfg, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CONFIG_PATH)
        _cached_config = dict(new_cfg)
        try:
            _cached_mtime = os.path.getmtime(CONFIG_PATH)
        except Exception:
            _cached_mtime = time.time()
    return dict(new_cfg)


def render_admin_models_html() -> str:
    cfg = get_models_config()
    ag = html.escape(cfg["antigravity_model"])
    dr = html.escape(cfg["direct_model"])
    rt = html.escape(cfg["router_model"])
    upd = html.escape(cfg.get("updated_at") or "по умолчанию")

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Admin — Модели AI</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; background: #0d1117; color: #e6edf3; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; padding: 16px; }}
  .card {{ width: 100%; max-width: 460px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; box-shadow: 0 12px 32px rgba(0,0,0,0.45); }}
  h1 {{ margin: 0 0 4px 0; font-size: 18px; font-weight: 600; }}
  .sub {{ font-size: 12px; color: #8b949e; margin-bottom: 18px; }}
  .field {{ margin-bottom: 14px; }}
  label {{ display: block; font-size: 12px; color: #c9d1d9; margin-bottom: 5px; font-weight: 500; }}
  input {{ width: 100%; background: #0d1117; color: #f0f6fc; border: 1px solid #30363d; border-radius: 5px; padding: 8px 10px; font-size: 13px; font-family: ui-monospace, SFMono-Regular, Consolas, monospace; outline: none; }}
  input:focus {{ border-color: #58a6ff; }}
  .hint {{ font-size: 11px; color: #6e7681; margin-top: 4px; }}
  button {{ width: 100%; background: #238636; color: #fff; border: 1px solid #2ea043; border-radius: 5px; padding: 9px 14px; font-size: 13px; font-weight: 600; cursor: pointer; margin-top: 4px; }}
  button:hover {{ background: #2ea043; }}
  button:disabled {{ opacity: 0.6; cursor: not-allowed; }}
  .status {{ margin-top: 12px; font-size: 12px; min-height: 18px; }}
  .ok {{ color: #3fb950; }}
  .err {{ color: #f85149; }}
</style>
</head>
<body>
<div class="card">
  <h1>Настройка моделей AI</h1>
  <div class="sub">Последнее обновление: <span id="upd">{upd}</span></div>

  <datalist id="model-presets">
    <option value="gemini-3.5-flash-lite">
    <option value="gemini-3.6-flash">
    <option value="gemini-3.8-flash">
    <option value="gemini-4-hydrogen">
    <option value="gemini-2.5-flash">
  </datalist>

  <div class="field">
    <label for="ag-model">Antigravity (песочница / файлы)</label>
    <input id="ag-model" list="model-presets" value="{ag}" placeholder="gemini-3.5-flash-lite">
  </div>

  <div class="field">
    <label for="dr-model">Direct (быстрый стрим)</label>
    <input id="dr-model" list="model-presets" value="{dr}" placeholder="gemini-3.5-flash-lite">
  </div>

  <div class="field">
    <label for="rt-model">Router (классификатор)</label>
    <input id="rt-model" list="model-presets" value="{rt}" placeholder="gemini-3.5-flash-lite">
  </div>

  <div class="field">
    <label for="admin-key">Пароль админа (ADMIN_SECRET или JWT_SECRET)</label>
    <input id="admin-key" type="password" placeholder="Сохраняется в браузере">
    <div class="hint">Формат моделей: gemini-3.8-flash, gemini-3.5-flash-lite и т.д.</div>
  </div>

  <button id="save-btn">Сохранить модели</button>
  <div id="status" class="status"></div>
</div>

<script>
  const keyInput = document.getElementById('admin-key');
  keyInput.value = localStorage.getItem('ag_admin_key') || '';

  document.getElementById('save-btn').onclick = async () => {{
    const btn = document.getElementById('save-btn');
    const st = document.getElementById('status');
    const adminKey = keyInput.value.trim();
    if (adminKey) localStorage.setItem('ag_admin_key', adminKey);

    btn.disabled = true;
    st.className = 'status';
    st.textContent = 'Сохранение...';

    try {{
      const resp = await fetch('/admin/models', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{
          admin_key: adminKey,
          antigravity_model: document.getElementById('ag-model').value,
          direct_model: document.getElementById('dr-model').value,
          router_model: document.getElementById('rt-model').value
        }})
      }});
      const json = await resp.json();
      if (!resp.ok || json.status !== 'success') {{
        throw new Error(json.message || ('HTTP ' + resp.status));
      }}
      document.getElementById('ag-model').value = json.config.antigravity_model;
      document.getElementById('dr-model').value = json.config.direct_model;
      document.getElementById('rt-model').value = json.config.router_model;
      document.getElementById('upd').textContent = json.config.updated_at;
      st.className = 'status ok';
      st.textContent = 'Сохранено. Модели применены без перезапуска.';
    }} catch (e) {{
      st.className = 'status err';
      st.textContent = 'Ошибка: ' + e.message;
    }} finally {{
      btn.disabled = false;
    }}
  }};
</script>
</body>
</html>"""