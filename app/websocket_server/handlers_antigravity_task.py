import re
import json
import time
import asyncio
import logging
import aiomysql

from app.database.connection import get_async_db_connection
from app.services.ai_service import ai_instance
from app.websocket_server.utils import async_send
from app.websocket_server.antigravity_storage import (
    antigravity_user_sockets,
    active_antigravity_tasks,
    get_least_used_key_index,
    send_antigravity_event,
    load_chat_state,
    save_chat_state,
    detect_requested_ide_tag,
    build_synced_meta,
    build_server_file_manifest,
)

logger = logging.getLogger("WS_Server")


async def handle_antigravity_cancel_task(websocket, data):
    chat_uid = data.get("chat_id")
    ui_msg_id = data.get("ui_msg_id")
    logger.info(f"🛑 [WS CANCEL REQUEST] Запрос отмены для чата: {chat_uid}")

    if chat_uid and chat_uid in active_antigravity_tasks:
        task = active_antigravity_tasks[chat_uid]
        if not task.done():
            task.cancel()
            logger.info(f"🛑 [WS CANCEL] Таск успешно отменен в памяти: {chat_uid}")

    await async_send(websocket, {
        "type": "antigravity_cancelled",
        "chat_id": chat_uid,
        "ui_msg_id": ui_msg_id
    })


async def handle_antigravity_task(websocket, data):
    prompt = data.get("prompt", "").strip()
    is_tool_response = bool(data.get("is_tool_response", False))
    db_summary = (data.get("db_summary") or "").strip()
    target_files = data.get("target_files", [])
    user_token = data.get("user_token")
    workspace_path = data.get("workspace_path")
    chat_uid = data.get("chat_id") or data.get("chat_uid") or getattr(websocket, "active_chat_uid", None)

    if user_token:
        antigravity_user_sockets[user_token] = websocket
    if not chat_uid:
        chat_uid = f"chat_{int(time.time() * 1000)}"
    websocket.active_chat_uid = chat_uid

    ui_msg_id = data.get("ui_msg_id", str(int(time.time() * 1000)))
    if not prompt:
        return

    logger.info(f"📥 [WS IN] Чат: {chat_uid} | Роль: {'tool' if is_tool_response else 'user'} | Задача: '{prompt[:70]}' | ui_msg_id: {ui_msg_id}")

    chat_state = load_chat_state(chat_uid)
    if data.get("project_tree"):
        chat_state["project_tree"] = str(data["project_tree"])[:10000]
        chat_state["tree_updated_at"] = time.time()
        save_chat_state(chat_uid, chat_state)

    if not is_tool_response:
        clean_title_text = re.sub(r"\[Прикреплен.*?\]\s*Ссылка:.*?\n\n", "", prompt, flags=re.DOTALL).strip()
        if not clean_title_text:
            clean_title_text = prompt
        auto_title = clean_title_text[:30].replace("\n", " ").strip()
        if len(clean_title_text) > 30:
            auto_title += "..."
    else:
        auto_title = "Новый диалог"

    msg_role = "tool" if is_tool_response else "user"
    last_tag = chat_state.get("last_requested_tag")

    if is_tool_response and last_tag == "GET_TREE":
        chat_state["project_tree"] = prompt[:10000]
        chat_state["tree_updated_at"] = time.time()
        chat_state["last_requested_tag"] = None
        save_chat_state(chat_uid, chat_state)
        text_for_db = "[Дерево проекта обновлено и сохранено на сервере]"
    else:
        if is_tool_response:
            chat_state["last_requested_tag"] = None
            save_chat_state(chat_uid, chat_state)
        text_for_db = db_summary if (is_tool_response and db_summary) else prompt

    conn = None; cursor = None
    assigned_key = None
    last_engine = None
    need_title_update = False
    recent_history = []
    synced_files_dict = {}

    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)

        await cursor.execute(
            "SELECT key_index, title, last_engine, synced_files FROM antigravity_chats WHERE chat_uid = %s",
            (chat_uid,)
        )
        chat_row = await cursor.fetchone()

        if chat_row:
            assigned_key = chat_row["key_index"]
            last_engine = chat_row.get("last_engine")
            if chat_row.get("title") in ("Новый диалог", None, "") and not is_tool_response:
                need_title_update = True
            if chat_row.get("synced_files"):
                try:
                    synced_files_dict = (
                        json.loads(chat_row["synced_files"])
                        if isinstance(chat_row["synced_files"], str)
                        else dict(chat_row["synced_files"])
                    )
                except Exception:
                    synced_files_dict = {}
        else:
            assigned_key = await get_least_used_key_index(cursor)
            await cursor.execute(
                """INSERT INTO antigravity_chats (chat_uid, title, key_index, user_token, workspace_path) 
                   VALUES (%s, %s, %s, %s, %s)""",
                (chat_uid, auto_title, assigned_key, user_token, workspace_path)
            )
            await conn.commit()

        await cursor.execute(
            "SELECT role, text FROM antigravity_messages WHERE chat_uid = %s ORDER BY id DESC LIMIT 12",
            (chat_uid,)
        )
        recent_history = list(reversed(await cursor.fetchall() or []))

        await cursor.execute(
            "INSERT INTO antigravity_messages (chat_uid, role, text) VALUES (%s, %s, %s)",
            (chat_uid, msg_role, text_for_db)
        )
        await conn.commit()

    except Exception as dbe:
        logger.error(f"[DB Error Before Task] {dbe}", exc_info=True)
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()

    async def send_status_update(status_payload):
        await send_antigravity_event(user_token, websocket, {
            "type": "antigravity_status",
            "ui_msg_id": ui_msg_id,
            "chat_id": chat_uid,
            **status_payload
        })

    async def send_chunk_update(delta: str):
        if not delta:
            return
        await send_antigravity_event(user_token, websocket, {
            "type": "antigravity_chunk",
            "ui_msg_id": ui_msg_id,
            "chat_id": chat_uid,
            "delta": delta
        })

    active_antigravity_tasks[chat_uid] = asyncio.current_task()

    try:
        project_tree = chat_state.get("project_tree")
        file_manifest = build_server_file_manifest(chat_uid, synced_files_dict)

        # Все новые сообщения всегда идут в DIRECT.
        # Напрямую в ANTIGRAVITY возвращается только ответ IDE на тег, вызванный самим ANTIGRAVITY.
        run_direct_first = not (is_tool_response and last_engine == "ANTIGRAVITY")
        used_engine = "DIRECT" if run_direct_first else "ANTIGRAVITY"
        result = None

        ag_task_prompt = prompt
        ag_selected_files = list(target_files) if target_files else []
        ag_need_network = False

        if run_direct_first:
            direct_res = await ai_instance.run_direct(
                prompt_text=prompt,
                recent_history=recent_history,
                project_tree=project_tree,
                file_manifest=file_manifest,
                status_callback=send_status_update,
                chunk_callback=send_chunk_update,
                assigned_key=assigned_key,
            )
            if direct_res.get("escalate"):
                logger.info("⬆️ [DIRECT -> ANTIGRAVITY] Передача задачи в песочницу")
                used_engine = "ANTIGRAVITY"
                if direct_res.get("ag_task"):
                    ag_task_prompt = direct_res["ag_task"]
                if direct_res.get("ag_files"):
                    ag_selected_files = direct_res["ag_files"]
                ag_need_network = bool(direct_res.get("need_network", False))
            else:
                result = {
                    "text": direct_res["text"],
                    "file_urls": {},
                    "environment_id": None,
                    "interaction_id": None,
                    "assigned_key": direct_res.get("assigned_key", assigned_key),
                    "usage": direct_res.get("usage", {}),
                }

        if result is None:
            used_engine = "ANTIGRAVITY"
            result = await ai_instance.run_antigravity_agent(
                prompt_text=ag_task_prompt,
                status_callback=send_status_update,
                assigned_key=assigned_key,
                chat_uid=chat_uid,
                recent_history=recent_history,
                project_tree=project_tree,
                file_manifest=file_manifest,
                selected_files=ag_selected_files,
                need_network=ag_need_network,
                max_total_tokens=60_000,
            )

        new_env_id = result.get("environment_id")
        agent_reply_text = result["text"]
        final_assigned_key = result.get("assigned_key", assigned_key)
        last_in_tokens = int((result.get("usage") or {}).get("input", 0) or 0)

        next_tag = detect_requested_ide_tag(agent_reply_text)
        if next_tag:
            chat_state["last_requested_tag"] = next_tag
            save_chat_state(chat_uid, chat_state)

        logger.info(f"📤 [DONE] ({used_engine}) Ответ готов ({len(agent_reply_text)} симв.): '{agent_reply_text[:60]}...'")

        conn = None; cursor = None
        try:
            conn = await get_async_db_connection()
            cursor = await conn.cursor()

            new_files = result.get("file_urls", {})
            synced_str = None
            if new_files:
                merged_files = dict(synced_files_dict)
                merged_files.update(new_files)
                synced_str = json.dumps(merged_files, ensure_ascii=False)

            if need_title_update:
                await cursor.execute(
                    """UPDATE antigravity_chats 
                       SET last_interaction_id = NULL, environment_id = COALESCE(%s, environment_id), key_index = %s, title = %s, 
                           synced_files = COALESCE(%s, synced_files), last_engine = %s, last_input_tokens = %s, updated_at = NOW() 
                       WHERE chat_uid = %s""",
                    (new_env_id, final_assigned_key, auto_title, synced_str, used_engine, last_in_tokens, chat_uid)
                )
            else:
                await cursor.execute(
                    """UPDATE antigravity_chats 
                       SET last_interaction_id = NULL, environment_id = COALESCE(%s, environment_id), key_index = %s, 
                           synced_files = COALESCE(%s, synced_files), last_engine = %s, last_input_tokens = %s, updated_at = NOW() 
                       WHERE chat_uid = %s""",
                    (new_env_id, final_assigned_key, synced_str, used_engine, last_in_tokens, chat_uid)
                )

            await cursor.execute(
                "INSERT INTO antigravity_messages (chat_uid, role, text) VALUES (%s, 'agent', %s)",
                (chat_uid, agent_reply_text)
            )
            await conn.commit()
        except Exception as dbe2:
            logger.error(f"[DB Error After Task] {dbe2}")
        finally:
            if cursor: await cursor.close()
            if conn: conn.close()

        await send_antigravity_event(user_token, websocket, {
            "type": "antigravity_result",
            "ui_msg_id": ui_msg_id,
            "text": agent_reply_text,
            "file_urls": result.get("file_urls", {}),
            "file_meta": build_synced_meta(chat_uid, result.get("file_urls", {})),
            "environment_id": new_env_id,
            "interaction_id": None,
            "chat_id": chat_uid
        })

    except asyncio.CancelledError:
        logger.warning(f"🛑 [WS Antigravity] Задача {chat_uid} прервана! Очистка...")
        conn = None; cursor = None
        try:
            conn = await get_async_db_connection()
            cursor = await conn.cursor()
            await cursor.execute(
                "DELETE FROM antigravity_messages WHERE chat_uid = %s AND role IN ('user', 'tool') ORDER BY id DESC LIMIT 1",
                (chat_uid,)
            )
            await cursor.execute(
                "UPDATE antigravity_chats SET last_interaction_id = NULL, environment_id = NULL WHERE chat_uid = %s",
                (chat_uid,)
            )
            await conn.commit()
        except Exception as cle:
            logger.warning(f"[WS Cleanup Error] {cle}")
        finally:
            if cursor: await cursor.close()
            if conn: conn.close()

        await send_antigravity_event(user_token, websocket, {
            "type": "antigravity_cancelled",
            "chat_id": chat_uid,
            "ui_msg_id": ui_msg_id
        })
    except Exception as e:
        logger.error(f"[WS Antigravity Error] {e}", exc_info=True)
        await send_antigravity_event(user_token, websocket, {
            "type": "antigravity_error",
            "ui_msg_id": ui_msg_id,
            "error": str(e)
        })
    finally:
        active_antigravity_tasks.pop(chat_uid, None)