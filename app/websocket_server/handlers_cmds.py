import base64
import json
import logging
import asyncio
import aiomysql
from datetime import datetime
from app.database.connection import get_async_db_connection
from app.services.ai_service import ai_instance
from app.websocket_server import state
from app.websocket_server.utils import async_send, get_device_type, get_accessible_devices
import time
import os
import shutil
import re

logger = logging.getLogger("WS_Server")

HISTORY_LIMIT = 10 
active_media_queues = {} 
device_audio_locks = {}

def get_device_audio_lock(client_id: int) -> asyncio.Lock:
    if client_id not in device_audio_locks:
        device_audio_locks[client_id] = asyncio.Lock()
    return device_audio_locks[client_id]

# Полный список поддерживаемых команд для управления устройствами
ACTION_DESCRIPTIONS = {
    "открытие ссылки": "строго полный URL адрес (например https://youtube.com)",
    "открытие файла": "строго точное имя программы или путь (например 'steam', 'telegram', 'calc', 'pubg')",
    "завершение процесса": "имя процесса (без .exe)",
    "напечатать текст": "любой текст для набора на клавиатуре",
    "нажать кнопку мыши": "строго одно из: 'лкм', 'пкм', 'скм'",
    "переместить мышь": "строго координаты x и y через запятую (например: 500, 300)",
    "уведомление": "текст уведомления",
    "музыка": "строго одно из: 'включить', 'выключить', 'следующий', 'предыдущий'",
    "смена голоса": "строго одно из: Aoede, Puck, Kore, Charon",
    "очистка истории": "любой текст",
    "изменение громкости": "целое число от 0 до 100",
    "изменение яркости": "целое число от 0 до 100",
    "check_network_devices": "любой текст (возвращает список доступных устройств)",
    "get_running_processes": "любой текст (возвращает список запущенных процессов)",
    "get_installed_programs": "любой текст (возвращает список программ)",
    "выключить микрофон": "любой текст (доступно только на веб-сайте)",
    "название диалога": "краткое название для текущего диалога (1-3 слова)",
    "движение": "строго одно из: 'вперед', 'назад', 'влево', 'вправо'"
}

ALL_ACTIONS = list(ACTION_DESCRIPTIONS.keys())

def get_action_strings(action_keys):
    caps = []
    for k in action_keys:
        desc = ACTION_DESCRIPTIONS.get(k, "любой текст")
        caps.append(f"- {k} (принимает: {desc})")
    return "\n".join(caps), ", ".join(action_keys)

async def handle_audio_chunk(websocket, data):
    ui_msg_id = data.get("ui_msg_id")
    if ui_msg_id in active_media_queues:
        chunk = base64.b64decode(data.get("audio_base64", ""))
        active_media_queues[ui_msg_id].put_nowait({"type": "audio", "data": chunk})

async def handle_video_chunk(websocket, data):
    ui_msg_id = data.get("ui_msg_id")
    if ui_msg_id in active_media_queues:
        chunk = base64.b64decode(data.get("video_base64", ""))
        active_media_queues[ui_msg_id].put_nowait({"type": "video", "data": chunk})

async def handle_audio_end(websocket, data):
    ui_msg_id = data.get("ui_msg_id")
    if ui_msg_id in active_media_queues:
        active_media_queues[ui_msg_id].put_nowait(None)
        active_media_queues.pop(ui_msg_id, None)

def get_target_websocket(user_id, target_device_name, fallback_ws):
    """Поиск целевого веб-сокета в RAM по имени или типу устройства."""
    if not target_device_name or not user_id:
        return fallback_ws
    clean_name = target_device_name.lower().strip()
    
    user_ws_set = state.user_connections.get(user_id, set())
    for ws in user_ws_set:
        session = state.ws_sessions.get(ws)
        if not session: continue
        d_name = session.get('device_name', '').lower()
        d_type = session.get('device_type', '').lower()
        
        # 1. Прямое совпадение по имени устройства
        if clean_name in d_name or d_name in clean_name:
            return ws
        # 2. Совпадение по типу
        if ("комп" in clean_name or "пк" in clean_name or "pc" in clean_name) and (d_type == "pc" or "комп" in d_name or "pc" in d_name):
            return ws
        if ("телефон" in clean_name or "phone" in clean_name) and (d_type == "phone" or "телефон" in d_name):
            return ws
        if ("робот" in clean_name or "robot" in clean_name) and (d_type == "robot" or "робот" in d_name):
            return ws
        if ("браузер" in clean_name or "web" in clean_name) and (d_type == "web" or "браузер" in d_name):
            return ws
            
    return fallback_ws


# =========================================================================
# 1. ПЕРВИЧНЫЙ АГЕНТ (ФАСАД) С ПРЯМЫМ УПРАВЛЕНИЕМ УСТРОЙСТВАМИ
# =========================================================================
async def handle_command(websocket, data):
    conn = None; cursor = None; user_msg_id = None; bot_message_id = None; dialog_id = None
    audio_chunks_count = 0; has_commands = False
    final_user_text_full = ""; final_bot_text_full = ""

    try:
        command = data.get('command', '').strip()
        name = data.get('name', 'Пятница')
        voice_name = data.get('voice_type', 'Aoede')
        screenshot_base64 = data.get('screenshot')
        audio_base64 = data.get('audio_base64') 
        ui_msg_id = data.get('ui_msg_id')
        message_history = data.get('message_history', [])
        
        is_streaming = data.get('stream_audio', False)
        media_queue = asyncio.Queue() if is_streaming else None
        
        if is_streaming and ui_msg_id:
            active_media_queues[ui_msg_id] = media_queue
            if audio_base64:
                media_queue.put_nowait({"type": "audio", "data": base64.b64decode(audio_base64)})
        
        session = state.ws_sessions.get(websocket, {})
        user_id = session.get('user_id')
        sender_name = session.get('device_name') or data.get('device_name') or data.get('DeviceName') or 'Компьютер'
        device_type_code = session.get('device_type') or data.get('device_type') or 'pc'
        device_type = get_device_type(type_str=device_type_code)

        image_bytes = base64.b64decode(screenshot_base64) if screenshot_base64 else None
        audio_bytes = base64.b64decode(audio_base64) if audio_base64 else None

        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        db_user_text = command if command else ("[Аудиосообщение]" if (audio_bytes or is_streaming) else "🖼️ [Фото]")
        
        logger.info("\n" + "="*50)
        logger.info(f"[ФАСАД] Инициатор: {sender_name} ({device_type}) | Стрим: {is_streaming}")

        client_dialog_id = data.get('dialog_id')
        sender_ws = websocket
        
        formatted_history = []

        if user_id:
            create_new = False
            if 'dialog_id' in data and client_dialog_id is None: create_new = True
            elif client_dialog_id:
                await cursor.execute("SELECT id FROM dialogs WHERE id = %s AND user_id = %s", (client_dialog_id, user_id))
                if not await cursor.fetchone(): create_new = True
                else: dialog_id = client_dialog_id
            else:
                await cursor.execute("SELECT id FROM dialogs WHERE user_id = %s ORDER BY created_at DESC LIMIT 1", (user_id,))
                dlg = await cursor.fetchone()
                if dlg: dialog_id = dlg['id']
                else: create_new = True
                    
            if create_new:
                dialog_name = "Новый диалог"
                await cursor.execute("INSERT INTO dialogs (name, user_id) VALUES (%s, %s)", (dialog_name, user_id))
                dialog_id = cursor.lastrowid
                await conn.commit()
                if sender_ws: await async_send(sender_ws, {"type": "dialog_created", "dialog_id": dialog_id, "name": dialog_name})

            if dialog_id:
                await cursor.execute(
                    "INSERT INTO messages (sender, device_name, text, dialog_id) VALUES ('Вы', %s, %s, %s)",
                    (sender_name, db_user_text, dialog_id)
                )
                user_msg_id = cursor.lastrowid
                await cursor.execute(
                    "INSERT INTO messages (sender, device_name, text, dialog_id) VALUES ('Бот', NULL, '', %s)",
                    (dialog_id,)
                )
                bot_message_id = cursor.lastrowid
                await conn.commit()

                await cursor.execute("""
                    SELECT sender, text FROM messages
                    WHERE dialog_id = %s AND id < %s ORDER BY created_at ASC
                """, (dialog_id, user_msg_id))
                
                raw_history = await cursor.fetchall()
                if raw_history:
                    for msg in raw_history[-HISTORY_LIMIT:]:
                        role = "user" if msg['sender'] == 'Вы' else "model"
                        formatted_history.append({"role": role, "parts": [{"text": msg['text']}]})
        else:
            user_msg_id = int(datetime.now().timestamp() * 1000)
            if message_history:
                for m in message_history[-HISTORY_LIMIT:]:
                    role = "user" if m.get('role') == 'user' else "model"
                    formatted_history.append({"role": role, "parts": [{"text": m.get('content', '')}]})

        if sender_ws and ui_msg_id and dialog_id:
            await async_send(sender_ws, {"type": "msg_id_map", "ui_msg_id": ui_msg_id, "user_msg_id": user_msg_id, "bot_msg_id": bot_message_id})

        # === ПОДТЯГИВАЕМ ОНЛАЙН-УСТРОЙСТВА ИЗ ОПЕРАТИВНОЙ ПАМЯТИ ===
        accessible_devices = await get_accessible_devices(None, websocket, user_id)
        online_devices_list = [f"- {dev}" for dev in accessible_devices]
        online_devices_list.append(f"- {sender_name} ({device_type}, текущее устройство)")
        devices_online_str = "\n".join(online_devices_list)

        caps_text, allowed_actions = get_action_strings(ALL_ACTIONS)

        system_instruction = f"""Ты — ИИ-помощник {name}. Твой собеседник говорит через устройство: {sender_name} (Тип: {device_type}).
Тебе в реальном времени передается видеопоток с камеры пользователя.

ДОСТУПНЫЕ УСТРОЙСТВА В СЕТИ (ОНЛАЙН СЕЙЧАС):
{devices_online_str}

ПРАВИЛА И ПРЯМОЕ УПРАВЛЕНИЕ:
1. ВСЕГДА НА ЛЮБОЙ ЗАПРОС ОТВЕЧАЙ ГОЛОСОМ! Даже если вызываешь команду, обязательно скажи коротко: "Секунду", "Открываю", "Делаю". Никогда не молчи!
2. ПРЯМОЕ УПРАВЛЕНИЕ: Ты управляешь устройствами НАПРЯМУЮ через инструмент `send_device_commands`.
   - В параметре `target_device` ОБЯЗАТЕЛЬНО указывай точное имя целевого устройства из списка онлайн-устройств (например '{sender_name}' или компьютер пользователя).
   - Если пользователь говорит с телефона: "Открой ютуб на компе" или "Включи музыку на компьютере" — сразу вызывай `send_device_commands` с `target_device='имя компьютера'` и нужным действием!
3. ЗАПУСК ИГР И ПРОГРАММ: Чтобы запустить приложение/игру на компьютере (пабг, стим, браузер, телеграм), вызывай action_type='открытие файла' с именем программы ('pubg', 'steam', 'telegram') или 'открытие ссылки' для сайтов.
4. СТОП-СЛОВА: Если пользователь ругается, говорит "хватит", "отмена", "не надо", "в пизду", "отбой" — СТРОГО ЗАПРЕЩЕНО вызывать любые команды! Скажи: "Понял, отбой".
5. КРАТКОСТЬ: Говори кратко и емко (1-2 простых предложения). Никаких длинных паст!

Доступные действия для устройств:
{caps_text}"""
        
        prompt_text_to_send = f"[ЗАПРОС]: {command}" if command else None
        logger.info(f"[API] Отправляю в Фасад (Gemini Live 3.1)... Онлайн устройств: {len(online_devices_list)}")

        if is_streaming and media_queue:
            generator = ai_instance.generate_audio_stream_realtime(
                prompt_text=prompt_text_to_send, system_instruction=system_instruction, allowed_actions=allowed_actions, media_queue=media_queue, formatted_history=formatted_history, voice_name=voice_name, assistant_name=name
            )
        else:
            generator = ai_instance.generate_audio_stream(
                prompt_text=prompt_text_to_send, system_instruction=system_instruction, allowed_actions=allowed_actions, audio_bytes=audio_bytes, image_bytes=image_bytes, formatted_history=formatted_history, voice_name=voice_name, assistant_name=name
            )

        audio_lock = get_device_audio_lock(id(websocket))
        async with audio_lock:
            async for chunk in generator:
                if chunk["type"] == "user_text":
                    final_user_text_full += chunk["text"] + " "
                    logger.info(f"[STT] Пользователь: {chunk['text'].strip()}")
                    if sender_ws: await async_send(sender_ws, {"type": "user_transcription", "ui_msg_id": ui_msg_id, "text": final_user_text_full.strip()})

                elif chunk["type"] == "bot_text":
                    text_chunk = chunk["text"]
                    if "call:send_device_commands" in text_chunk or "send_device_commands{" in text_chunk: continue
                    final_bot_text_full += text_chunk + " "
                    logger.info(f"[TTS] Фасад: {text_chunk.strip()}")
                    if sender_ws: await async_send(sender_ws, {"type": "new_message", "message_id": bot_message_id, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": text_chunk, "actions": []})

                elif chunk["type"] == "commands":
                    if chunk["commands"]: has_commands = True
                    extracted_commands = chunk["commands"]
                    
                    for cmd in extracted_commands:
                        target_device_name = cmd.get('target_device', '').strip()
                        actions = cmd.get('actions', [])
                        if not actions: continue

                        # Находим сокет целевого устройства в оперативной памяти
                        target_ws = get_target_websocket(user_id, target_device_name, fallback_ws=websocket)

                        real_actions = []
                        for act in actions:
                            act_type = act.get('action_type')
                            act_val = act.get('action_value')

                            if act_type == "название диалога" and dialog_id:
                                new_name = str(act_val).strip()
                                await cursor.execute("UPDATE dialogs SET name = %s WHERE id = %s", (new_name, dialog_id))
                                await conn.commit()
                                if sender_ws: await async_send(sender_ws, {"type": "dialog_renamed", "dialog_id": dialog_id, "name": new_name})
                            else:
                                real_actions.append(act)

                        # Отправляем команду целевому устройству
                        if real_actions:
                            if target_ws:
                                logger.info(f"🚀 [DIRECT DISPATCH] Фасад отправил {len(real_actions)} действий на '{target_device_name or sender_name}'")
                                await async_send(target_ws, {
                                    "type": "new_message",
                                    "message_id": bot_message_id if target_ws == websocket else None,
                                    "user_msg_id": user_msg_id if target_ws == websocket else None,
                                    "sender": "Бот" if target_ws == websocket else sender_name,
                                    "text": "",
                                    "actions": real_actions
                                })
                            else:
                                logger.warning(f"⚠️ [DIRECT DISPATCH] Устройство '{target_device_name}' не в сети!")

                elif chunk["type"] == "audio":
                    audio_chunks_count += 1
                    if sender_ws: await async_send(sender_ws, {"type": "audio_chunk", "audio_base64": base64.b64encode(chunk["data"]).decode('utf-8')})
        
        if dialog_id and user_msg_id and bot_message_id:
            if (audio_bytes or is_streaming):
                if final_user_text_full.strip(): await cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_user_text_full.strip(), user_msg_id))
            if not final_bot_text_full.strip() and audio_chunks_count == 0 and not has_commands:
                await cursor.execute("DELETE FROM messages WHERE id IN (%s, %s)", (bot_message_id, user_msg_id))
            else:
                await cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_bot_text_full.strip(), bot_message_id))
            await conn.commit()
            
        if not dialog_id:
            if not final_bot_text_full.strip() and audio_chunks_count == 0 and not has_commands:
                if sender_ws: await async_send(sender_ws, {"type": "delete_message", "ui_msg_id": ui_msg_id})
            else:
                if sender_ws: await async_send(sender_ws, {"type": "new_message", "message_id": None, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": "", "actions": []})
        elif sender_ws:
            await async_send(sender_ws, {"type": "new_message", "message_id": bot_message_id, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": "", "actions": []})

        logger.info(f"[DONE] Фасад отработал.\n" + "="*50)

    except Exception as e:
        logger.error(f"[ERROR] {e}", exc_info=True)
        try:
            if cursor and bot_message_id and user_msg_id:
                await cursor.execute("DELETE FROM messages WHERE id IN (%s, %s)", (bot_message_id, user_msg_id)); await conn.commit()
            sender_ws = websocket
            if sender_ws: await async_send(sender_ws, {"type": "delete_message", "ui_msg_id": ui_msg_id})
        except: pass
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()
        active_media_queues.pop(data.get('ui_msg_id', ''), None)

async def get_least_used_key_index(cursor):
    total_keys = len(ai_instance.api_keys)
    if total_keys <= 1: return 0

    await cursor.execute("SELECT key_index, COUNT(*) as cnt FROM antigravity_chats GROUP BY key_index")
    rows = await cursor.fetchall()
    
    key_counts = {i: 0 for i in range(total_keys)}
    for r in rows:
        idx = r['key_index']
        if idx in key_counts: key_counts[idx] = r['cnt']

    usable_keys = [i for i in range(total_keys) if ai_instance.key_pool.is_usable(i)]
    if not usable_keys:
        usable_keys = list(range(total_keys))

    best_key = min(usable_keys, key=lambda k: key_counts.get(k, 0))
    return best_key

active_antigravity_tasks = {}

# ==============================================================================
# CRUD ДИАЛОГОВ И СООБЩЕНИЙ ANTIGRAVITY (С ПРИВЯЗКОЙ К АККАУНТУ И ПАПКЕ)
# ==============================================================================

async def handle_antigravity_get_chats(websocket, data):
    user_token = data.get("user_token")
    conn = None; cursor = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        if not user_token:
            await async_send(websocket, {"type": "antigravity_chats_list", "chats": []})
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
            if c.get("updated_at"): c["updated_at"] = str(c["updated_at"])

        await async_send(websocket, {
            "type": "antigravity_chats_list",
            "chats": chats or []
        })
    except Exception as e:
        logger.error(f"[DB get_chats error] {e}")
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()


async def handle_antigravity_get_messages(websocket, data):
    chat_uid = data.get("chat_uid")
    if not chat_uid: return
    conn = None; cursor = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        await cursor.execute(
            "SELECT role, text, created_at FROM antigravity_messages WHERE chat_uid = %s ORDER BY created_at ASC",
            (chat_uid,)
        )
        msgs = await cursor.fetchall()
        for m in msgs:
            if m.get("created_at"): m["created_at"] = str(m["created_at"])

        await cursor.execute("SELECT synced_files, workspace_path FROM antigravity_chats WHERE chat_uid = %s", (chat_uid,))
        chat_row = await cursor.fetchone()
        synced_files = {}
        ws_path = None
        if chat_row:
            ws_path = chat_row.get("workspace_path")
            if chat_row.get("synced_files"):
                try:
                    synced_files = json.loads(chat_row["synced_files"]) if isinstance(chat_row["synced_files"], str) else chat_row["synced_files"]
                except Exception: pass

        await async_send(websocket, {
            "type": "antigravity_chat_history",
            "chat_uid": chat_uid,
            "workspace_path": ws_path,
            "messages": msgs or [],
            "synced_files": synced_files or {}
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
    if not chat_uid: return
    conn = None; cursor = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor()
        await cursor.execute("DELETE FROM antigravity_messages WHERE chat_uid = %s", (chat_uid,))
        await cursor.execute("DELETE FROM antigravity_chats WHERE chat_uid = %s", (chat_uid,))
        await conn.commit()

        upload_dir = os.path.join('/opt/friday/storage/uploads', chat_uid)
        if os.path.exists(upload_dir):
            shutil.rmtree(upload_dir, ignore_errors=True)

        await async_send(websocket, {
            "type": "antigravity_chat_deleted",
            "chat_uid": chat_uid
        })
    except Exception as e:
        logger.error(f"[DB delete_chat error] {e}")
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()


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
    target_files = data.get("target_files", [])
    input_files = data.get("input_files", {})
    user_token = data.get("user_token")
    workspace_path = data.get("workspace_path")
    chat_uid = data.get("chat_id") or data.get("chat_uid") or getattr(websocket, "active_chat_uid", None)
    if not chat_uid:
        chat_uid = f"chat_{int(time.time() * 1000)}"
    websocket.active_chat_uid = chat_uid

    ui_msg_id = data.get("ui_msg_id", str(int(time.time() * 1000)))
    if not prompt: return

    logger.info(f"📥 [WS IN] Чат: {chat_uid} | Задача: '{prompt[:70]}' | ui_msg_id: {ui_msg_id}")

    conn = None; cursor = None
    assigned_key = None
    prev_interaction_id = None
    environment_id = None
    need_title_update = False
    recent_history = []

    clean_title_text = re.sub(r"\[Прикреплен.*?\]\s*Ссылка:.*?\n\n", "", prompt, flags=re.DOTALL).strip()
    if not clean_title_text: clean_title_text = prompt
    auto_title = clean_title_text[:30].replace("\n", " ").strip()
    if len(clean_title_text) > 30: auto_title += "..."

    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)

        await cursor.execute(
            "SELECT key_index, last_interaction_id, environment_id, title FROM antigravity_chats WHERE chat_uid = %s",
            (chat_uid,)
        )
        chat_row = await cursor.fetchone()

        if chat_row:
            assigned_key = chat_row["key_index"]
            prev_interaction_id = chat_row["last_interaction_id"]
            environment_id = chat_row["environment_id"]
            if chat_row.get("title") in ("Новый диалог", None, ""):
                need_title_update = True
        else:
            assigned_key = await get_least_used_key_index(cursor)
            await cursor.execute(
                """INSERT INTO antigravity_chats (chat_uid, title, key_index, user_token, workspace_path) 
                   VALUES (%s, %s, %s, %s, %s)""",
                (chat_uid, auto_title, assigned_key, user_token, workspace_path)
            )
            await conn.commit()

        logger.info(f"📋 [DB STATE] Чат: {chat_uid} | Ключ #{assigned_key} | prev_id: {prev_interaction_id[:12] if prev_interaction_id else 'None'} | env: {environment_id[:12] if environment_id else 'None'}")

        await cursor.execute(
            "SELECT role, text FROM antigravity_messages WHERE chat_uid = %s ORDER BY created_at ASC",
            (chat_uid,)
        )
        recent_history = await cursor.fetchall() or []

        await cursor.execute(
            "INSERT INTO antigravity_messages (chat_uid, role, text) VALUES (%s, 'user', %s)",
            (chat_uid, prompt)
        )
        await conn.commit()

    except Exception as dbe:
        logger.error(f"[DB Error Before Task] {dbe}", exc_info=True)
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()

    async def send_status_update(status_payload):
        await async_send(websocket, {
            "type": "antigravity_status",
            "ui_msg_id": ui_msg_id,
            **status_payload
        })

    active_antigravity_tasks[chat_uid] = asyncio.current_task()

    try:
        result = await ai_instance.run_antigravity_agent(
            prompt_text=prompt,
            status_callback=send_status_update,
            target_files=target_files,
            input_files=input_files,
            previous_interaction_id=prev_interaction_id,
            environment_id=environment_id,
            assigned_key=assigned_key,
            chat_uid=chat_uid,
            recent_history=recent_history
        )

        new_interaction_id = result["interaction_id"]
        new_env_id = result["environment_id"]
        agent_reply_text = result["text"]
        final_assigned_key = result.get("assigned_key", assigned_key)

        logger.info(f"📤 [ANTIGRAVITY DONE] Ответ готов ({len(agent_reply_text)} симв.): '{agent_reply_text[:60]}...'")

        try:
            conn = await get_async_db_connection()
            cursor = await conn.cursor()

            new_files = result.get("file_urls", {})
            synced_str = json.dumps(new_files, ensure_ascii=False) if new_files else None
            
            if need_title_update:
                await cursor.execute(
                    """UPDATE antigravity_chats 
                       SET last_interaction_id = %s, environment_id = %s, key_index = %s, title = %s, 
                           synced_files = COALESCE(%s, synced_files), updated_at = NOW() 
                       WHERE chat_uid = %s""",
                    (new_interaction_id, new_env_id, final_assigned_key, auto_title, synced_str, chat_uid)
                )
            else:
                await cursor.execute(
                    """UPDATE antigravity_chats 
                       SET last_interaction_id = %s, environment_id = %s, key_index = %s, 
                           synced_files = COALESCE(%s, synced_files), updated_at = NOW() 
                       WHERE chat_uid = %s""",
                    (new_interaction_id, new_env_id, final_assigned_key, synced_str, chat_uid)
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

        await async_send(websocket, {
            "type": "antigravity_result",
            "ui_msg_id": ui_msg_id,
            "text": agent_reply_text,
            "file_urls": result.get("file_urls", {}),
            "environment_id": new_env_id,
            "interaction_id": new_interaction_id,
            "chat_id": chat_uid
        })

    except asyncio.CancelledError:
        logger.warning(f"🛑 [WS Antigravity] Задача {chat_uid} прервана! Очистка контекста...")
        try:
            conn = await get_async_db_connection()
            cursor = await conn.cursor()
            await cursor.execute(
                "DELETE FROM antigravity_messages WHERE chat_uid = %s AND role = 'user' ORDER BY id DESC LIMIT 1",
                (chat_uid,)
            )
            await cursor.execute(
                "UPDATE antigravity_chats SET last_interaction_id = NULL, environment_id = NULL WHERE chat_uid = %s",
                (chat_uid,)
            )
            await conn.commit()
            logger.info(f"🧹 [CLEANUP SUCCESS] Окружение Google сброшено в NULL для чата {chat_uid}.")
        except Exception as cle:
            logger.warning(f"[WS Cleanup Error] {cle}")
        finally:
            if cursor: await cursor.close()
            if conn: conn.close()

        await async_send(websocket, {
            "type": "antigravity_cancelled",
            "chat_id": chat_uid,
            "ui_msg_id": ui_msg_id
        })
    except Exception as e:
        logger.error(f"[WS Antigravity Error] {e}", exc_info=True)
        await async_send(websocket, {
            "type": "antigravity_error",
            "ui_msg_id": ui_msg_id,
            "error": str(e)
        })
    finally:
        active_antigravity_tasks.pop(chat_uid, None)
