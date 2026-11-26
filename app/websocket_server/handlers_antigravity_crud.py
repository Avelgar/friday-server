import os
import json
import time
import shutil
import hashlib
import logging
import urllib.parse
import aiomysql

from app.database.connection import get_async_db_connection
from app.services.ai_service import ai_instance
from app.websocket_server.utils import async_send
from app.websocket_server.antigravity_storage import (
    UPLOADS_DIR,
    antigravity_user_sockets,
    get_least_used_key_index,
    build_synced_meta,
    delete_chat_state,
)

logger = logging.getLogger("WS_Server")


async def handle_antigravity_register_client(websocket, data):
    user_token = data.get("user_token")
    if user_token:
        antigravity_user_sockets[user_token] = websocket


async def handle_antigravity_delete_synced_file(websocket, data):
    chat_uid = data.get("chat_uid")
    file_path = (data.get("file_path") or "").strip().replace("\\", "/").lstrip("./")
    if not chat_uid or not file_path:
        return

    try:
        base_dir = os.path.abspath(os.path.join(UPLOADS_DIR, os.path.basename(chat_uid)))
        target_path = os.path.abspath(os.path.join(base_dir, file_path))
        if target_path.startswith(base_dir + os.sep) and os.path.exists(target_path):
            if os.path.isdir(target_path):
                shutil.rmtree(target_path, ignore_errors=True)
            else:
                os.remove(target_path)
            logger.info(f"🗑️ [SERVER FILE DELETED] {target_path}")
    except Exception as e:
        logger.warning(f"⚠️ [SERVER FILE DELETE ERR] {e}")

    conn = None; cursor = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        await cursor.execute("SELECT synced_files FROM antigravity_chats WHERE chat_uid = %s", (chat_uid,))
        row = await cursor.fetchone()
        if row and row.get("synced_files"):
            sf = json.loads(row["synced_files"]) if isinstance(row["synced_files"], str) else row["synced_files"]
            if isinstance(sf, dict):
                keys_to_del = [k for k in sf.keys() if k.replace("\\", "/").lstrip("./") == file_path]
                for k in keys_to_del:
                    sf.pop(k, None)
                await cursor.execute(
                    "UPDATE antigravity_chats SET synced_files = %s WHERE chat_uid = %s",
                    (json.dumps(sf, ensure_ascii=False), chat_uid)
                )
                await conn.commit()
                logger.info(f"🧹 [DB SYNCED CLEANED] Удален '{file_path}' из чата {chat_uid}")
    except Exception as dbe:
        logger.error(f"[DB delete_synced_file error] {dbe}")
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()


async def handle_antigravity_save_system_log(websocket, data):
    chat_uid = data.get("chat_uid")
    text = (data.get("text") or "").strip()
    if not chat_uid or not text:
        return
    conn = None; cursor = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor()
        await cursor.execute(
            "INSERT INTO antigravity_messages (chat_uid, role, text) VALUES (%s, 'system', %s)",
            (chat_uid, text)
        )
        await conn.commit()
    except Exception as e:
        logger.error(f"[DB save_system_log error] {e}")
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()


async def handle_antigravity_get_chats(websocket, data):
    user_token = data.get("user_token")
    conn = None; cursor = None
    if user_token:
        antigravity_user_sockets[user_token] = websocket
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)

        if not user_token:
            await async_send(websocket, {
                "type": "antigravity_chats_list",
                "chats": [],
                "silent": bool(data.get("silent", False))
            })
            return

        await cursor.execute(
            """INSERT INTO antigravity_users (user_token, username) VALUES (%s, %s)
               ON DUPLICATE KEY UPDATE username = VALUES(username)""",
            (user_token, data.get("username", "Developer"))
        )
        await conn.commit()

        await cursor.execute(
            """SELECT chat_uid, title, key_index, last_interaction_id, environment_id, 
                      workspace_path, updated_at 
               FROM antigravity_chats 
               WHERE user_token = %s 
               ORDER BY updated_at DESC""",
            (user_token,)
        )
        chats = await cursor.fetchall()
        for c in chats:
            if c.get("updated_at"):
                c["updated_at"] = str(c["updated_at"])

        await async_send(websocket, {
            "type": "antigravity_chats_list",
            "chats": chats or [],
            "silent": bool(data.get("silent", False))
        })
    except Exception as e:
        logger.error(f"[DB get_chats error] {e}")
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()


async def handle_antigravity_get_messages(websocket, data):
    chat_uid = data.get("chat_uid")
    if not chat_uid:
        return
    conn = None; cursor = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)

        await cursor.execute(
            "SELECT role, text, created_at FROM antigravity_messages WHERE chat_uid = %s ORDER BY id ASC",
            (chat_uid,)
        )
        msgs = await cursor.fetchall()
        for m in msgs:
            if m.get("created_at"):
                m["created_at"] = str(m["created_at"])

        await cursor.execute("SELECT synced_files, workspace_path FROM antigravity_chats WHERE chat_uid = %s", (chat_uid,))
        chat_row = await cursor.fetchone()
        synced_files = {}
        if chat_row and chat_row.get("synced_files"):
            try:
                synced_files = json.loads(chat_row["synced_files"]) if isinstance(chat_row["synced_files"], str) else chat_row["synced_files"]
            except Exception:
                pass

        await async_send(websocket, {
            "type": "antigravity_chat_history",
            "chat_uid": chat_uid,
            "messages": msgs or [],
            "synced_files": synced_files or {},
            "synced_meta": build_synced_meta(chat_uid, synced_files or {})
        })
    except Exception as e:
        logger.error(f"[DB get_messages error] {e}")
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()


async def handle_antigravity_create_chat(websocket, data):
    chat_uid = data.get("chat_uid") or f"chat_{int(time.time() * 1000)}"
    title = data.get("title") or "Новый диалог"
    user_token = data.get("user_token")
    workspace_path = data.get("workspace_path")
    if user_token:
        antigravity_user_sockets[user_token] = websocket
    conn = None; cursor = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        assigned_key = await get_least_used_key_index(cursor)

        await cursor.execute(
            """INSERT INTO antigravity_chats (chat_uid, title, key_index, user_token, workspace_path) 
               VALUES (%s, %s, %s, %s, %s)""",
            (chat_uid, title, assigned_key, user_token, workspace_path)
        )
        await conn.commit()

        await async_send(websocket, {
            "type": "antigravity_chat_created",
            "chat": {
                "chat_uid": chat_uid,
                "title": title,
                "key_index": assigned_key,
                "user_token": user_token,
                "workspace_path": workspace_path,
                "last_interaction_id": None,
                "environment_id": None
            }
        })
    except Exception as e:
        logger.error(f"[DB create_chat error] {e}")
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()


async def handle_antigravity_delete_chat(websocket, data):
    chat_uid = data.get("chat_uid")
    if not chat_uid:
        return
    conn = None; cursor = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor()
        await cursor.execute("DELETE FROM antigravity_messages WHERE chat_uid = %s", (chat_uid,))
        await cursor.execute("DELETE FROM antigravity_chats WHERE chat_uid = %s", (chat_uid,))
        await conn.commit()

        upload_dir = os.path.join(UPLOADS_DIR, os.path.basename(chat_uid))
        if os.path.exists(upload_dir):
            shutil.rmtree(upload_dir, ignore_errors=True)
        delete_chat_state(chat_uid)

        await async_send(websocket, {
            "type": "antigravity_chat_deleted",
            "chat_uid": chat_uid
        })
    except Exception as e:
        logger.error(f"[DB delete_chat error] {e}")
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()


async def handle_antigravity_list_server_files(websocket, data):
    chat_uid = data.get("chat_uid")
    if not chat_uid:
        return
    safe_uid = os.path.basename(chat_uid)
    base_dir = os.path.abspath(os.path.join(UPLOADS_DIR, safe_uid))
    files = []

    if os.path.exists(base_dir):
        for root, _, filenames in os.walk(base_dir):
            for fname in filenames:
                full_p = os.path.abspath(os.path.join(root, fname))
                if not full_p.startswith(base_dir + os.sep):
                    continue
                rel_p = os.path.relpath(full_p, base_dir).replace("\\", "/")
                try:
                    st = os.stat(full_p)
                    f_hash = None
                    if st.st_size <= 50 * 1024 * 1024:
                        h = hashlib.sha256()
                        with open(full_p, "rb") as hf:
                            for chunk in iter(lambda: hf.read(65536), b""):
                                h.update(chunk)
                        f_hash = h.hexdigest()
                    files.append({
                        "path": rel_p,
                        "name": fname,
                        "size": st.st_size,
                        "mtime": int(st.st_mtime),
                        "sha256": f_hash,
                        "url": f"https://friday-assistant.ru/uploads/{safe_uid}/{urllib.parse.quote(rel_p)}?v={int(st.st_mtime)}"
                    })
                except Exception:
                    pass
    files.sort(key=lambda x: x["path"])
    await async_send(websocket, {
        "type": "server_files_list",
        "chat_uid": chat_uid,
        "files": files
    })


async def handle_antigravity_delete_server_file(websocket, data):
    await handle_antigravity_delete_synced_file(websocket, data)
    chat_uid = data.get("chat_uid")
    file_path = (data.get("file_path") or "").strip().replace("\\", "/").lstrip("./")
    if chat_uid and file_path:
        base_dir = os.path.abspath(os.path.join(UPLOADS_DIR, os.path.basename(chat_uid)))
        parent = os.path.dirname(os.path.abspath(os.path.join(base_dir, file_path)))
        while parent.startswith(base_dir + os.sep):
            try:
                if not os.listdir(parent):
                    os.rmdir(parent)
                    parent = os.path.dirname(parent)
                else:
                    break
            except Exception:
                break
    await async_send(websocket, {
        "type": "server_file_deleted",
        "chat_uid": chat_uid,
        "file_path": file_path
    })


async def handle_antigravity_list_env_files(websocket, data):
    chat_uid = data.get("chat_uid")
    if not chat_uid:
        return
    conn = None; cursor = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        await cursor.execute("SELECT environment_id, key_index FROM antigravity_chats WHERE chat_uid = %s", (chat_uid,))
        row = await cursor.fetchone()
        env_id = row.get("environment_id") if row else None
        key_idx = row.get("key_index") if row else None

        if not env_id:
            await async_send(websocket, {
                "type": "env_files_list",
                "chat_uid": chat_uid,
                "has_env": False,
                "files": []
            })
            return

        files = await ai_instance.list_environment_files(env_id, key_index=key_idx)
        await async_send(websocket, {
            "type": "env_files_list",
            "chat_uid": chat_uid,
            "has_env": True,
            "env_id": env_id,
            "files": files
        })
    except Exception as e:
        logger.error(f"[ENV LIST ERROR] {e}")
        await async_send(websocket, {
            "type": "env_files_list",
            "chat_uid": chat_uid,
            "has_env": True,
            "error": str(e),
            "files": []
        })
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()


async def handle_antigravity_download_env_file(websocket, data):
    chat_uid = data.get("chat_uid")
    remote_path = (data.get("remote_path") or "").strip()
    if not chat_uid or not remote_path:
        return
    conn = None; cursor = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        await cursor.execute("SELECT environment_id, key_index, synced_files FROM antigravity_chats WHERE chat_uid = %s", (chat_uid,))
        row = await cursor.fetchone()
        if not row or not row.get("environment_id"):
            await async_send(websocket, {
                "type": "fm_error",
                "chat_uid": chat_uid,
                "error": "Песочница не создана для этого диалога"
            })
            return

        res = await ai_instance.download_environment_file(
            row["environment_id"], remote_path, chat_uid, key_index=row.get("key_index")
        )

        sf = {}
        if row.get("synced_files"):
            try:
                sf = json.loads(row["synced_files"]) if isinstance(row["synced_files"], str) else dict(row["synced_files"])
            except Exception:
                sf = {}
        sf[res["path"]] = res["url"]
        await cursor.execute(
            "UPDATE antigravity_chats SET synced_files = %s WHERE chat_uid = %s",
            (json.dumps(sf, ensure_ascii=False), chat_uid)
        )
        await conn.commit()

        await async_send(websocket, {
            "type": "env_file_downloaded",
            "chat_uid": chat_uid,
            "file_path": res["path"],
            "url": res["url"],
            "size": res["size"]
        })
    except Exception as e:
        logger.error(f"[ENV DOWNLOAD ERROR] {e}")
        await async_send(websocket, {
            "type": "fm_error",
            "chat_uid": chat_uid,
            "error": f"Ошибка скачивания из песочницы: {e}"
        })
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()


async def handle_antigravity_upload_env_file(websocket, data):
    chat_uid = data.get("chat_uid")
    local_filename = os.path.basename(data.get("local_filename") or "")
    remote_path = (data.get("remote_path") or "").strip()
    if not chat_uid or not local_filename or not remote_path:
        return
    conn = None; cursor = None
    try:
        safe_uid = os.path.basename(chat_uid)
        base_dir = os.path.abspath(os.path.join(UPLOADS_DIR, safe_uid))
        local_server_path = os.path.abspath(os.path.join(base_dir, local_filename))
        if not local_server_path.startswith(base_dir + os.sep) or not os.path.exists(local_server_path):
            raise FileNotFoundError("Загруженный файл не найден на сервере")

        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        await cursor.execute("SELECT environment_id, key_index FROM antigravity_chats WHERE chat_uid = %s", (chat_uid,))
        row = await cursor.fetchone()
        if not row or not row.get("environment_id"):
            raise RuntimeError("Песочница ещё не создана")

        uploaded_path = await ai_instance.upload_file_to_environment(
            row["environment_id"], local_server_path, remote_path, key_index=row.get("key_index")
        )
        await async_send(websocket, {
            "type": "env_file_uploaded",
            "chat_uid": chat_uid,
            "remote_path": uploaded_path
        })
    except Exception as e:
        logger.error(f"[ENV UPLOAD ERROR] {e}")
        await async_send(websocket, {
            "type": "fm_error",
            "chat_uid": chat_uid,
            "error": f"Ошибка загрузки в песочницу: {e}"
        })
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()