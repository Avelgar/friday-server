import os
import re
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


# Теги, которые исполняет IDE. RUN_COMMAND намеренно НЕ отслеживается: раньше он
# оставался в last_requested_tag после любого curl'а, и следующие 12 сообщений
# модели шли в ветку «ответ песочницы» вместо обычной логики.
TRACKED_TAGS = ("GET_FILES", "GET_TREE", "ASK_USER")
_TAG_PATTERNS = {
    "GET_TREE": re.compile(r"\[GET_TREE\]"),
    "GET_FILES": re.compile(r"\[GET_FILES:\s*([^\]]+)\]", re.IGNORECASE),
    "ASK_USER": re.compile(r"\[ASK_USER:\s*[^\]]*\]", re.IGNORECASE),
}


def detect_requested_ide_tag(text: str) -> str | None:
    t = text or ""
    for tag in TRACKED_TAGS:
        if _TAG_PATTERNS[tag].search(t):
            return tag
    return None


def _split_need_paths(raw: str) -> list[str]:
    """
    Режет тело [GET_FILES: ...] на пути, НЕ ломая имена с запятыми.

    Расширение VS Code режет по запятой — поэтому имя вида
    «Шаблоны_КРКП_Листы_задания,_титульный_лист_...docx» превращалось в два
    несуществующих файла, IDE отвечала «не найден в проекте», сервер снова
    просил тот же файл, и получался бесконечный цикл запросов.
    """
    parts = [p.strip() for p in (raw or "").split(",")]
    merged: list[str] = []
    for part in parts:
        if not part:
            continue
        # у предыдущего куска нет расширения => это склейка одного имени
        if merged and not re.search(r"\.[A-Za-z0-9]{1,6}$", merged[-1]):
            merged[-1] = merged[-1] + "," + part
        else:
            merged.append(part)
    return merged


def partition_get_files(paths: list) -> tuple[str, list]:
    """
    Делит запрошенные пути на «можно запросить» и «нельзя».

    Протокол [GET_FILES: a, b] режет тело по запятой, поэтому файл с запятой
    в имени (например «Шаблоны_...,_титульный_лист....docx») приходит в IDE
    как ДВА несуществующих файла. IDE отвечает «не найден в проекте», сервер
    просит снова — и так по кругу. Такие файлы честно возвращаем отдельно,
    чтобы обработчик объяснил проблему пользователю, а не зациклился.
    """
    clean = [str(p).strip() for p in (paths or []) if str(p).strip()]
    ok = [p for p in clean if "," not in p]
    blocked = [p for p in clean if "," in p]
    tag = f"[GET_FILES: {', '.join(ok)}]" if ok else ""
    return tag, blocked


def detect_run_command_urls(text: str) -> list[str]:
    """
    Достаёт URL из [RUN_COMMAND: ...] вида `curl ... "https://..." -o js/app.js`
    (и просто [RUN_COMMAND: curl URL]). Нужно, чтобы перехватить бесполезные
    «curl по уже скачанному файлу» и превратить их в обычную эскалацию.
    """
    urls: list[str] = []
    for m in re.finditer(r"\[RUN_COMMAND:\s*([^\]]*)\]", text or "", flags=re.IGNORECASE):
        body = m.group(1)
        for u in re.findall(r"https?://[^\s\"']+", body):
            u = u.rstrip(".,;)")
            if u not in urls:
                urls.append(u)
    return urls


def _canonical_upload_path(rel_p: str) -> str:
    """Превращает префиксные имена вроде mwxjjq7x_js__entities.js в чистый путь js/entities.js."""
    m = re.match(r"^[a-z0-9]{7,10}_(.+__.+)$", rel_p)
    if m:
        return m.group(1).replace("__", "/").lstrip("/")
    return rel_p


def build_synced_meta(chat_uid: str, synced_files: dict) -> dict:
    meta = {}
    if not synced_files or not isinstance(synced_files, dict):
        return meta
    safe_uid = os.path.basename(chat_uid or 'common')
    base_dir = os.path.abspath(os.path.join(UPLOADS_DIR, safe_uid))
    for rel_p, f_url in synced_files.items():
        norm_rel = str(rel_p).replace("\\", "/")
        norm_rel = re.sub(r"^(\./)+", "", norm_rel).lstrip("/")
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
            clean_k = re.sub(r"^(\./)+", "", str(k).replace("\\", "/")).lstrip("/")
            url_map[clean_k] = v

    raw_items = []
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
                canonical = _canonical_upload_path(rel_p)
                raw_items.append({
                    "path": canonical,
                    "raw_path": rel_p,
                    "is_prefixed": canonical != rel_p,
                    "size": st.st_size,
                    "lines": lines_cnt,
                    "mtime": int(st.st_mtime),
                    "url": f_url,
                })
            except Exception:
                pass

    # Сортируем от самых свежих к старым и оставляем по 1 самой новой версии каждого файла
    raw_items.sort(key=lambda x: (x["mtime"], not x["is_prefixed"]), reverse=True)
    deduped = []
    seen_paths = set()
    for item in raw_items:
        if item["path"] in seen_paths:
            continue
        seen_paths.add(item["path"])
        deduped.append({
            "path": item["path"],
            "size": item["size"],
            "lines": item["lines"],
            "mtime": item["mtime"],
            "url": item["url"],
        })

    return deduped