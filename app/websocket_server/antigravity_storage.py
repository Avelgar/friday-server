import os
import json
import time
import hashlib
import logging
import urllib.parse

from app.services.ai_service import ai_instance
from app.websocket_server import state
from app.websocket_server.utils import async_send

logger = logging.getLogger("WS_Server")

CHAT_STATE_DIR = "/opt/friday/storage/chat_state"
UPLOADS_DIR = "/opt/friday/storage/uploads"

antigravity_user_sockets = {}
active_antigravity_tasks = {}


async def get_least_used_key_index(cursor=None):
    return ai_instance.key_pool.get_best_key(model="antigravity")


async def send_antigravity_event(user_token, fallback_ws, payload):
    ws = antigravity_user_sockets.get(user_token) if user_token else None
    if ws and ws in state.active_connections:
        await async_send(ws, payload)
        return
    if fallback_ws and fallback_ws in state.active_connections:
        await async_send(fallback_ws, payload)


def _chat_state_path(chat_uid: str) -> str:
    safe_uid = os.path.basename(chat_uid or "common")
    return os.path.join(CHAT_STATE_DIR, f"{safe_uid}.json")


def load_chat_state(chat_uid: str) -> dict:
    p = _chat_state_path(chat_uid)
    try:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_chat_state(chat_uid: str, state_data: dict) -> None:
    try:
        os.makedirs(CHAT_STATE_DIR, exist_ok=True)
        p = _chat_state_path(chat_uid)
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state_data, f, ensure_ascii=False)
        os.replace(tmp, p)
    except Exception as e:
        logger.warning(f"⚠️ [CHAT STATE SAVE ERR] {e}")


def delete_chat_state(chat_uid: str) -> None:
    try:
        p = _chat_state_path(chat_uid)
        if os.path.exists(p):
            os.remove(p)
    except Exception as e:
        logger.warning(f"⚠️ [CHAT STATE DELETE ERR] {e}")


def detect_requested_ide_tag(text: str) -> str | None:
    t = text or ""
    if "[GET_TREE]" in t:
        return "GET_TREE"
    if "[GET_FILES:" in t:
        return "GET_FILES"
    if "[RUN_COMMAND:" in t:
        return "RUN_COMMAND"
    if "[ASK_USER:" in t:
        return "ASK_USER"
    return None


def build_synced_meta(chat_uid: str, synced_files: dict) -> dict:
    meta = {}
    if not synced_files or not isinstance(synced_files, dict):
        return meta
    safe_uid = os.path.basename(chat_uid or "common")
    base_dir = os.path.abspath(os.path.join(UPLOADS_DIR, safe_uid))
    for rel_p, f_url in synced_files.items():
        norm_rel = str(rel_p).replace("\\", "/").lstrip("./")
        local_p = os.path.abspath(os.path.join(base_dir, norm_rel))
        if not local_p.startswith(base_dir + os.sep) or not os.path.isfile(local_p):
            continue
        f_size = None
        f_hash = None
        try:
            f_size = os.path.getsize(local_p)
            if f_size <= 50 * 1024 * 1024:
                h = hashlib.sha256()
                with open(local_p, "rb") as hf:
                    for chunk in iter(lambda: hf.read(65536), b""):
                        h.update(chunk)
                f_hash = h.hexdigest()
        except Exception:
            pass
        meta[norm_rel] = {
            "url": f_url,
            "size": f_size,
            "sha256": f_hash,
        }
    return meta


def build_server_file_manifest(chat_uid: str, synced_files: dict | None = None) -> list:
    safe_uid = os.path.basename(chat_uid or "common")
    base_dir = os.path.abspath(os.path.join(UPLOADS_DIR, safe_uid))
    if not os.path.isdir(base_dir):
        return []

    url_map = {}
    if isinstance(synced_files, dict):
        for k, v in synced_files.items():
            url_map[str(k).replace("\\", "/").lstrip("./")] = v

    manifest = []
    for root, _, filenames in os.walk(base_dir):
        for fname in filenames:
            full_p = os.path.abspath(os.path.join(root, fname))
            if not full_p.startswith(base_dir + os.sep):
                continue
            rel_p = os.path.relpath(full_p, base_dir).replace("\\", "/")
            try:
                st = os.stat(full_p)
                lines_cnt = None
                if st.st_size <= 2 * 1024 * 1024:
                    with open(full_p, "rb") as tf:
                        chunk = tf.read()
                    if b"\x00" not in chunk[:4096]:
                        lines_cnt = chunk.count(b"\n") + (1 if chunk else 0)
                f_url = url_map.get(rel_p) or (
                    f"https://friday-assistant.ru/uploads/{safe_uid}/"
                    f"{urllib.parse.quote(rel_p)}?v={int(st.st_mtime)}"
                )
                manifest.append({
                    "path": rel_p,
                    "size": st.st_size,
                    "lines": lines_cnt,
                    "mtime": int(st.st_mtime),
                    "url": f_url,
                })
            except Exception:
                pass

    manifest.sort(key=lambda x: x["mtime"], reverse=True)
    return manifest