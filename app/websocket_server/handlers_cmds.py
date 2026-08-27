import base64
import json
import logging
import asyncio
import aiomysql
import hashlib
from datetime import datetime
from app.database.connection import get_async_db_connection
from app.services.ai_service import ai_instance
from app.websocket_server.state import mac_to_websocket, ws_to_mac
from app.websocket_server.utils import async_send, get_device_type, get_accessible_devices

logger = logging.getLogger("WS_Server")

HISTORY_LIMIT = 10 
active_media_queues = {} 

# =========================================================================
# ГЛОБАЛЬНЫЕ СЛОВАРИ ДЛЯ БЛОКИРОВОК И ПЕРЕХВАТОВ (THE BRAIN BRIDGE)
# =========================================================================
device_audio_locks = {}
pending_device_responses = {} # Здесь хранятся Future для ожидания ответов от C#

def get_device_audio_lock(mac: str) -> asyncio.Lock:
    if mac not in device_audio_locks:
        device_audio_locks[mac] = asyncio.Lock()
    return device_audio_locks[mac]

ACTION_DESCRIPTIONS = {
    "открытие ссылки": "строго полный URL адрес (например https://youtube.com)",
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
    "открытие файла": "строго точный абсолютный путь к программе/файлу",
    "завершение процесса": "имя процесса (без .exe)",
    "режим камеры": "любой текст",
    "выключить режим камеры": "любой текст",
    "выключить микрофон": "любой текст (доступно только на веб-сайте)",
    "голосовой ответ": "текст для озвучивания (Используется для финального ответа)",
    "название диалога": "краткое название для текущего диалога (1-3 слова).",
    "смена имени": "любой текст (новое имя для бота)",
    "движение": "строго одно из: 'вперед', 'назад', 'влево', 'вправо'",
    
    # --- НОВЫЕ ИНСТРУМЕНТЫ МАРШРУТИЗАЦИИ (ОРКЕСТРАЦИЯ) ---
    "delegate_to_brain": "строго подробное описание задачи. Вызывай НЕМЕДЛЕННО, если нужно работать с сетью, программами, процессами или другими устройствами.",
    "delegate_to_heavy_brain": "строго подробное описание задачи. Вызывай это, если тебе нужно визуально проанализировать экран пользователя (найти кнопку, посмотреть картинку) или физически поводить мышью/напечатать текст.",
    "request_screenshot": "Запросить актуальный скриншот с устройства, чтобы понять, куда кликать дальше",
    "task_completed": "Вызови, когда визуальная задача полностью выполнена, чтобы вернуть управление Быстрому Мозгу"
}

# =========================================================================
# ПОЛНЫЙ АРСЕНАЛ УСТРОЙСТВ
# =========================================================================
BASE_PC = [
    "открытие ссылки", "напечатать текст", "нажать кнопку мыши", "переместить мышь", 
    "уведомление", "музыка", "смена голоса", "очистка истории", 
    "изменение громкости", "изменение яркости", "check_network_devices", 
    "get_running_processes", "get_installed_programs", "открытие файла", "завершение процесса", "голосовой ответ"
]

BASE_PHONE = [
    "открытие ссылки", "изменение громкости", "изменение яркости", "музыка", 
    "очистка истории", "check_network_devices", "get_running_processes", "get_installed_programs", "голосовой ответ"
]

BASE_WEB = ["смена голоса", "выключить микрофон", "очистка истории", "check_network_devices", "голосовой ответ"]
BASE_PI = ["музыка", "очистка истории", "смена голоса", "движение", "check_network_devices", "голосовой ответ"]

# =========================================================================
# ФАСАДНЫЕ ИНСТРУМЕНТЫ (Доступно Gemini 3.1 Live для быстрой реакции)
# =========================================================================
FACADE_PC = ["музыка", "смена голоса", "очистка истории", "изменение громкости", "изменение яркости", "уведомление", "delegate_to_brain"]
FACADE_PHONE = ["музыка", "очистка истории", "изменение громкости", "изменение яркости", "delegate_to_brain"]
FACADE_WEB = ["смена голоса", "выключить микрофон", "очистка истории", "delegate_to_brain"]
FACADE_PI = ["музыка", "очистка истории", "смена голоса", "движение", "delegate_to_brain"]


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

# =========================================================================
# 1. ПЕРВИЧНЫЙ АГЕНТ (КОНТУР 1 - ФАСАД Gemini 3.1 Live)
# =========================================================================
async def handle_command(websocket, data):
    # -------------------------------------------------------------------------
    # МАГИЯ ПЕРЕХВАТА: Если это ответ от C# с процессами/программами для Мозга
    # -------------------------------------------------------------------------
    client_msg_id = str(data.get('user_msg_id', ''))
    if client_msg_id and client_msg_id in pending_device_responses:
        processes = data.get('processes')
        programs = data.get('programs')
        screenshot = data.get('screenshot')
        
        if processes is not None or programs is not None or screenshot is not None:
            logger.info(f"[BRIDGE] Перехват ответа от устройства! Возвращаем данные Мозгу.")
            future = pending_device_responses[client_msg_id]
            if not future.done():
                future.set_result(data)
            return  # ПРЕРЫВАЕМ ФАСАД, ЭТО ПАКЕТ ДЛЯ МОЗГА!
    # -------------------------------------------------------------------------

    conn = None; cursor = None; user_msg_id = None; bot_message_id = None; dialog_id = None
    audio_chunks_count = 0; has_commands = False
    final_user_text_full = ""; final_bot_text_full = ""
    pending_routes = []

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
        
        mac = ws_to_mac.get(websocket) or data.get('mac')
        if not mac and data.get('token'):
            mac = f"WEB{hashlib.md5(str(data.get('token')).encode()).hexdigest()[:13]}"
            
        image_bytes = base64.b64decode(screenshot_base64) if screenshot_base64 else None
        audio_bytes = base64.b64decode(audio_base64) if audio_base64 else None

        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        await cursor.execute("SELECT id, device_name, user_id FROM devices WHERE mac = %s", (mac,))
        sender_device = await cursor.fetchone()
        if not sender_device: raise Exception(f"Устройство отправителя не найдено (MAC: {mac})")
        
        sender_id = sender_device['id']; sender_name = sender_device['device_name']; device_type = get_device_type(mac)
        db_user_text = command if command else ("[Аудиосообщение]" if (audio_bytes or is_streaming) else "🖼️ [Фото]")
        
        logger.info("\n" + "="*50)
        logger.info(f"[ФАСАД] Инициатор: {sender_name} | Стрим: {is_streaming}")

        client_dialog_id = data.get('dialog_id')
        user_id = sender_device.get('user_id')
        sender_ws = mac_to_websocket.get(mac)
        
        is_new_dialog = False
        formatted_history = []

        if user_id:
            create_new = False
            if 'dialog_id' in data and client_dialog_id is None:
                create_new = True
            elif client_dialog_id:
                await cursor.execute("SELECT id FROM dialogs WHERE id = %s AND user_id = %s", (client_dialog_id, user_id))
                if not await cursor.fetchone():
                    create_new = True
                else:
                    dialog_id = client_dialog_id
            else:
                await cursor.execute("SELECT id FROM dialogs WHERE user_id = %s ORDER BY created_at DESC LIMIT 1", (user_id,))
                dlg = await cursor.fetchone()
                if dlg: dialog_id = dlg['id']
                else: create_new = True
                    
            if create_new:
                is_new_dialog = True
                dialog_name = "Новый диалог"
                await cursor.execute("INSERT INTO dialogs (name, user_id) VALUES (%s, %s)", (dialog_name, user_id))
                dialog_id = cursor.lastrowid
                await conn.commit()
                if sender_ws:
                    await async_send(sender_ws, {"type": "dialog_created", "dialog_id": dialog_id, "name": dialog_name})

            if dialog_id:
                await cursor.execute("INSERT INTO messages (send_type, text, recipient_device_id, dialog_id) VALUES ('Вы', %s, %s, %s)", (db_user_text, sender_id, dialog_id))
                user_msg_id = cursor.lastrowid
                await cursor.execute("INSERT INTO messages (send_type, text, recipient_device_id, dialog_id) VALUES ('Бот', '', %s, %s)", (sender_id, dialog_id))
                bot_message_id = cursor.lastrowid
                await conn.commit()

                await cursor.execute("""
                    SELECT CASE WHEN m.send_type = 'Вы' THEN 'Пользователь' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender_name, m.text
                    FROM messages m LEFT JOIN devices d ON m.send_type COLLATE utf8mb4_general_ci = CAST(d.id AS CHAR) COLLATE utf8mb4_general_ci AND m.send_type NOT IN ('Вы', 'Бот')
                    WHERE m.dialog_id = %s AND m.id < %s ORDER BY m.created_at ASC
                """, (dialog_id, user_msg_id))
                
                raw_history = await cursor.fetchall()
                if raw_history:
                    for msg in raw_history[-HISTORY_LIMIT:]:
                        role = "user" if msg['sender_name'] == 'Пользователь' else "model"
                        formatted_history.append({"role": role, "parts": [{"text": msg['text']}]})
        else:
            # Генерация ID гостевого сообщения для корректной работы Мозга
            user_msg_id = int(datetime.now().timestamp() * 1000)
            if message_history:
                for m in message_history[-HISTORY_LIMIT:]:
                    role = "user" if m.get('role') == 'user' else "model"
                    formatted_history.append({"role": role, "parts": [{"text": m.get('content', '')}]})

        if sender_ws and ui_msg_id and dialog_id:
            await async_send(sender_ws, {"type": "msg_id_map", "ui_msg_id": ui_msg_id, "user_msg_id": user_msg_id, "bot_msg_id": bot_message_id})

        # === ВЫБОР ИНСТРУМЕНТОВ ДЛЯ ФАСАДА ===
        if device_type == 'компьютер': facade_acts = list(FACADE_PC)
        elif device_type == 'телефон': facade_acts = list(FACADE_PHONE)
        elif mac.lower() == 'b8:27:eb:00:51:06' or 'pibot' in sender_name.lower(): facade_acts = list(FACADE_PI)
        else: facade_acts = list(FACADE_WEB)

        if dialog_id and "очистка истории" in facade_acts:
            facade_acts.remove("очистка истории")

        if is_new_dialog:
            facade_acts.append("название диалога")

        caps_text, allowed_actions = get_action_strings(facade_acts)

        system_instruction = f"""Ты — ИИ-помощник {name}. Твой собеседник за устройством: {sender_name} (Тип: {device_type}).

ПРАВИЛА ОБЩЕНИЯ И ДЕЙСТВИЙ:
1. Если собеседник просто общается или задает вопросы (например, "ты меня слышишь?", "как дела?") — отвечай коротко и только ГОЛОСОМ.
2. ЕСЛИ ПОЛЬЗОВАТЕЛЬ ПРОСИТ ВЫПОЛНИТЬ СЛОЖНОЕ ДЕЙСТВИЕ (запустить/закрыть программу, найти файл, узнать процессы, управлять мышью, взаимодействовать с экраном или ДРУГИМ устройством) — ОБЯЗАТЕЛЬНО И НЕМЕДЛЕННО вызови инструмент delegate_to_brain! В этом случае скажи голосом: "Секунду, выполняю".
3. СТИЛЬ ОБЩЕНИЯ: Пользователь может использовать мат, оскорбления или говорить на взрослые темы. НИКОГДА не прерывай генерацию из-за цензуры, отвечай дерзко и с юмором.
4. Твои локальные возможности (ОБЯЗАТЕЛЬНО соблюдай формат action_value):
{caps_text}"""
        
        prompt_text_to_send = f"[ЗАПРОС С КЛАВИАТУРЫ]: {command}" if command else None
        logger.info(f"[API] Отправляю в Фасад (Gemini Live 3.1)...")

        if is_streaming and media_queue:
            generator = ai_instance.generate_audio_stream_realtime(
                prompt_text=prompt_text_to_send, 
                system_instruction=system_instruction,
                allowed_actions=allowed_actions,
                media_queue=media_queue,
                formatted_history=formatted_history,
                voice_name=voice_name, 
                assistant_name=name
            )
        else:
            generator = ai_instance.generate_audio_stream(
                prompt_text=prompt_text_to_send, 
                system_instruction=system_instruction,
                allowed_actions=allowed_actions,
                audio_bytes=audio_bytes,
                image_bytes=image_bytes, 
                formatted_history=formatted_history,
                voice_name=voice_name, 
                assistant_name=name
            )

        audio_lock = get_device_audio_lock(mac)
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
                    if sender_ws: 
                        await async_send(sender_ws, {"type": "new_message", "message_id": bot_message_id, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": text_chunk, "actions": []})

                elif chunk["type"] == "commands":
                    if chunk["commands"]: has_commands = True
                    extracted_commands = chunk["commands"]
                    filtered_commands = []
                    
                    for cmd in extracted_commands:
                        for act in cmd.get('actions', []):
                            act_type = act.get('action_type')
                            act_val = act.get('action_value')
                            
                            # === ПЕРЕДАЧА ЗАДАЧИ МОЗГУ ===
                            if act_type in ["delegate_to_brain", "delegate_to_heavy_brain"]:
                                logger.warning(f"🧠 [ФАСАД ДЕЛЕГИРУЕТ ЗАДАЧУ МОЗГУ]: {act_val}")
                                pseudo_data = {
                                    "internal_routing": "brain_agent", 
                                    "task": act_val,
                                    "brain_type": act_type, # легкий или тяжелый
                                    "source_name": sender_name, 
                                    "mac": mac, 
                                    "user_id": sender_device.get('user_id'), 
                                    "user_msg_id": user_msg_id, 
                                    "voice_type": voice_name, 
                                    "message_history": message_history, 
                                    "dialog_id": dialog_id
                                }
                                pending_routes.append(pseudo_data)
                                
                            elif act_type == "название диалога" and dialog_id:
                                new_name = str(act_val).strip()
                                await cursor.execute("UPDATE dialogs SET name = %s WHERE id = %s", (new_name, dialog_id))
                                await conn.commit()
                                if sender_ws:
                                    await async_send(sender_ws, {"type": "dialog_renamed", "dialog_id": dialog_id, "name": new_name})
                            
                            else: 
                                filtered_commands.append({"target_device": cmd.get("target_device", ""), "actions": [act]})
                    
                    # Отправка мгновенных локальных команд Фасада
                    for cmd in filtered_commands:
                        target_device_name = cmd.get('target_device', '').strip()
                        actions = cmd.get('actions', [])
                        if not target_device_name or not actions: continue
                        
                        await cursor.execute("SELECT id, mac, device_name, user_id FROM devices WHERE device_name = %s", (target_device_name,))
                        target_device_info = await cursor.fetchone()
                        if not target_device_info:
                            await cursor.execute("SELECT id, mac, device_name, user_id FROM devices WHERE is_online = TRUE")
                            for d in await cursor.fetchall():
                                if d['device_name'].lower() in target_device_name.lower() or target_device_name.lower() in d['device_name'].lower():
                                    target_device_info = d; break
                        if not target_device_info: continue

                        target_mac = target_device_info['mac']
                        target_ws = mac_to_websocket.get(target_mac)
                        if target_ws:
                            await async_send(target_ws, {
                                "type": "new_message", 
                                "message_id": bot_message_id if target_mac == mac else None, 
                                "user_msg_id": user_msg_id if target_mac == mac else None, 
                                "sender": "Бот" if target_mac == mac else sender_name, 
                                "text": "", 
                                "actions": actions
                            })

                elif chunk["type"] == "audio":
                    audio_chunks_count += 1
                    if sender_ws: await async_send(sender_ws, {"type": "audio_chunk", "audio_base64": base64.b64encode(chunk["data"]).decode('utf-8')})
        
        if dialog_id and user_msg_id and bot_message_id:
            if (audio_bytes or is_streaming):
                if final_user_text_full.strip():
                    await cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_user_text_full.strip(), user_msg_id))
                
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
        
        # ЗАПУСК МОЗГА, ЕСЛИ ФАСАД СДЕЛАЛ ДЕЛЕГИРОВАНИЕ
        for route_data in pending_routes: await handle_target_command(websocket, route_data)

    except Exception as e:
        logger.error(f"[ERROR] {e}", exc_info=True)
        try:
            if cursor and bot_message_id and user_msg_id:
                await cursor.execute("DELETE FROM messages WHERE id IN (%s, %s)", (bot_message_id, user_msg_id)); await conn.commit()
            sender_ws = mac_to_websocket.get(mac)
            if sender_ws: await async_send(sender_ws, {"type": "delete_message", "ui_msg_id": ui_msg_id})
        except: pass
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()
        active_media_queues.pop(data.get('ui_msg_id', ''), None)


# =========================================================================
# 2. ОРКЕСТРАТОР (КОНТУР 2 и 3 - МОЗГ)
# =========================================================================
async def get_mac_by_name(cursor, device_name, fallback_mac):
    await cursor.execute("SELECT mac FROM devices WHERE device_name = %s", (device_name,))
    res = await cursor.fetchone()
    if res: return res['mac']
    await cursor.execute("SELECT mac, device_name FROM devices WHERE is_online = TRUE")
    for d in await cursor.fetchall():
        if d['device_name'].lower() in device_name.lower() or device_name.lower() in d['device_name'].lower():
            return d['mac']
    return fallback_mac


async def handle_target_command(websocket, data):
    """
    Stateful ReAct Оркестратор. Работает в цикле, общается с клиентами C# напрямую 
    через device_bridge_callback и дожидается ответов.
    """
    conn = None; cursor = None
    bot_message_id = None; source_ws = None
    
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        is_internal = data.get("internal_routing")
        if is_internal != "brain_agent":
            return

        brain_type = data.get("brain_type", "delegate_to_brain")
        voice_name = data.get('voice_type', 'Aoede')
        name = data.get('name', 'Пятница')
        task = data.get('task', '')
        source_name = data.get('source_name')
        mac = data.get('mac')
        user_id = data.get("user_id")
        user_msg_id = data.get("user_msg_id")
        dialog_id = data.get("dialog_id")
        message_history = data.get('message_history', [])

        await cursor.execute("SELECT id, device_name, mac FROM devices WHERE mac = %s", (mac,))
        source_device_info = await cursor.fetchone()
        if not source_device_info: return

        source_id = source_device_info['id']
        source_ws = mac_to_websocket.get(source_device_info['mac'])
        source_mac = source_device_info['mac']

        logger.info("\n" + "="*50)
        logger.info(f"[BRAIN] Подключение текстового мозга. Тип: {brain_type} | Задача: {task}")
        
        # --- Настройка Промптов и Моделей в зависимости от типа Мозга ---
        if brain_type == "delegate_to_heavy_brain":
            model_id = "gemini-3.7-flash"
            allowed_acts = ["нажать кнопку мыши", "переместить мышь", "напечатать текст", "голосовой ответ", "request_screenshot", "task_completed"]
            caps_text, allowed_actions = get_action_strings(allowed_acts)
            
            system_instruction = f"""Ты — Тяжелый Мозг-Зрение. Твой инициатор за устройством {source_name}.
Твоя задача: {task}. 
У тебя есть доступ к мыши, клавиатуре и экрану пользователя.
ПРАВИЛА:
1. Вызови action_type="request_screenshot", чтобы запросить скриншот и понять, где находятся элементы.
2. Изучив скриншот, отправь точные координаты (action_type="переместить мышь", затем "нажать кнопку мыши").
3. Обязательно вызови action_type="task_completed", когда закончишь визуальную задачу.
4. Вызови "голосовой ответ" для финальной озвучки итога работы.
Доступные инструменты:
{caps_text}"""

        else:
            model_id = "gemini-3.5-flash-lite"
            allowed_acts = list(set(BASE_PC + BASE_PHONE + BASE_PI + BASE_WEB))
            if "check_network_devices" in allowed_acts: allowed_acts.remove("check_network_devices")
            if dialog_id and "очистка истории" in allowed_acts: allowed_acts.remove("очистка истории")
            allowed_acts.append("check_network_devices")
            allowed_acts.append("delegate_to_heavy_brain")
            caps_text, allowed_actions = get_action_strings(allowed_acts)

            system_instruction = f"""Ты — Быстрый Мозг-Оркестратор. Твой инициатор за устройством {source_name}.
Твоя задача - пошагово выполнить системный запрос пользователя: {task}.

ПРАВИЛА ОРКЕСТРАЦИИ:
1. ИСПОЛЬЗУЙ ИНСТРУМЕНТЫ (Tools): Для поиска устройств вызови check_network_devices. Для работы с ПК вызови get_running_processes или get_installed_programs.
2. ПОЛНАЯ ТИШИНА: Если ты просто собираешь данные (запрашиваешь процессы/сеть), НЕ пиши текст ответа! Просто отправь инструмент и жди.
3. ТЯЖЕЛЫЙ МОЗГ: Если задача требует зрения, кликов по экрану или работы с интерфейсом (например, "лайкни видео", "открой Ютуб и нажми..."), вызови action_type="delegate_to_heavy_brain" с action_value="подробное описание визуальной задачи".
4. ФИНАЛ: Только когда задача ПОЛНОСТЬЮ завершена (программа закрыта, ответ найден), сгенерируй текстовый ответ (или вызови "голосовой ответ"), чтобы пользователь услышал результат.

Твои инструменты:
{caps_text}"""

        formatted_history = []
        if dialog_id:
            await cursor.execute("""
                SELECT CASE WHEN m.send_type = 'Вы' THEN 'Пользователь' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender_name, m.text
                FROM messages m LEFT JOIN devices d ON m.send_type COLLATE utf8mb4_general_ci = CAST(d.id AS CHAR) COLLATE utf8mb4_general_ci AND m.send_type NOT IN ('Вы', 'Бот')
                WHERE m.dialog_id = %s ORDER BY m.created_at ASC
            """, (dialog_id,))
            raw_history = await cursor.fetchall()
            if raw_history:
                for msg in raw_history[-HISTORY_LIMIT:]:
                    role = "user" if msg['sender_name'] == 'Пользователь' else "model"
                    formatted_history.append({"role": role, "parts": [{"text": msg['text']}]})
            
            await cursor.execute("INSERT INTO messages (send_type, text, recipient_device_id, dialog_id) VALUES ('Бот', '', %s, %s)", (source_id, dialog_id))
            bot_message_id = cursor.lastrowid
            await conn.commit()
        else:
            if message_history:
                for m in message_history[-HISTORY_LIMIT:]:
                    role = "user" if m.get('role') == 'user' else "model"
                    formatted_history.append({"role": role, "parts": [{"text": m.get('content', '')}]})

        # =========================================================================
        # МОСТ ОЖИДАНИЯ (Device Bridge)
        # =========================================================================
        # =========================================================================
        # МОСТ ОЖИДАНИЯ (Device Bridge)
        # =========================================================================
        async def device_bridge_callback(commands_to_execute):
            results = []
            for cmd in commands_to_execute:
                name_tool = cmd["name"]
                args = cmd.get("args", {})
                target_device_name = args.get("target_device", "")
                actions = args.get("actions", [])
                
                tool_resp = {"result": "success"}
                attached_image = None
                attached_res = ""
                
                for act in actions:
                    act_type = act.get("action_type")
                    act_val = act.get("action_value")
                    
                    if act_type == "check_network_devices":
                        logger.info("[BRIDGE] Опрос БД устройств...")
                        devices = await get_accessible_devices(cursor, source_mac, user_id)
                        tool_resp["devices_online"] = devices
                        
                    elif act_type in ["get_running_processes", "get_installed_programs", "request_screenshot"]:
                        target_mac = await get_mac_by_name(cursor, target_device_name, source_mac)
                        target_ws = mac_to_websocket.get(target_mac)
                        if target_ws:
                            future = asyncio.Future()
                            pending_device_responses[str(user_msg_id)] = future
                            
                            logger.info(f"[BRIDGE] Отправка запроса ({act_type}) на устройство {target_mac}. Ожидание ответа...")
                            await async_send(target_ws, {
                                "type": "new_message",
                                "user_msg_id": user_msg_id,
                                "actions": [act]
                            })
                            try:
                                client_data = await asyncio.wait_for(future, timeout=25.0)
                                logger.info(f"[BRIDGE] Ответ от устройства {target_mac} успешно получен!")
                                
                                if act_type == "get_running_processes": tool_resp["processes"] = client_data.get("processes")
                                elif act_type == "get_installed_programs": tool_resp["programs"] = client_data.get("programs")
                                elif act_type == "request_screenshot": 
                                    tool_resp["status"] = "Скриншот прикреплен к сообщению"
                                    # Вытаскиваем картинку для Тяжелого Мозга!
                                    attached_image = client_data.get("screenshot_base64_received")
                                    attached_res = client_data.get("screen_resolution", "unknown")
                            except asyncio.TimeoutError:
                                logger.warning(f"[BRIDGE] Таймаут ответа от устройства {target_mac}")
                                tool_resp["error"] = "Устройство не ответило вовремя"
                            finally:
                                pending_device_responses.pop(str(user_msg_id), None)
                        else:
                            tool_resp["error"] = "Устройство не в сети"
                            
                    elif act_type == "delegate_to_heavy_brain":
                        logger.info(f"[BRIDGE] Быстрый Мозг вызвал Тяжелый Мозг для задачи: {act_val}")
                        tool_resp["heavy_brain_status"] = "Задача передана Тяжелому мозгу. Сообщи пользователю, что ты приступаешь к выполнению."
                        
                    elif act_type != "task_completed":
                        # Локальные действия (клик, набор текста и т.д.)
                        target_mac = await get_mac_by_name(cursor, target_device_name, source_mac)
                        target_ws = mac_to_websocket.get(target_mac)
                        if target_ws:
                            await async_send(target_ws, {
                                "type": "new_message",
                                "user_msg_id": user_msg_id,
                                "actions": [act]
                            })
                            
                # Если была картинка, передаем её отдельно от JSON-ответа
                res_dict = {"name": name_tool, "response": tool_resp}
                if attached_image:
                    res_dict["attached_image_base64"] = attached_image
                    res_dict["attached_resolution"] = attached_res
                    
                results.append(res_dict)
                
            return results
        # =========================================================================
        # ВЫЗОВ НУЖНОГО МОЗГА
        # =========================================================================
        if brain_type == "delegate_to_heavy_brain":
            final_text = await ai_instance.execute_heavy_agent(
                prompt_text=f"Выполни визуальную задачу: {task}",
                system_instruction=system_instruction,
                allowed_actions=allowed_actions,
                formatted_history=formatted_history,
                device_bridge_callback=device_bridge_callback,
                model_id=model_id
            )
        else:
            final_text = await ai_instance.run_brain_orchestrator(
                prompt_text=f"Выполни задачу: {task}",
                system_instruction=system_instruction,
                allowed_actions=allowed_actions,
                formatted_history=formatted_history,
                device_bridge_callback=device_bridge_callback,
                model_id=model_id
            )

        # 2. ОЗВУЧКА РЕЗУЛЬТАТА И СНЯТИЕ БЛОКИРОВОК
        final_text = final_text.strip()
        if final_text:
            logger.info(f"[TTS] Мозг: {final_text}")
            audio_b64 = await ai_instance.generate_static_audio(final_text, voice_name, name)

            audio_lock = get_device_audio_lock(source_mac)
            async with audio_lock:
                if audio_b64 and source_ws:
                    await async_send(source_ws, {"type": "audio_chunk", "audio_base64": audio_b64})
                if source_ws:
                    await async_send(source_ws, {"type": "new_message", "message_id": bot_message_id, "ui_msg_id": str(bot_message_id) if bot_message_id else None, "sender": "Бот", "text": final_text, "actions": []})
                
                # Сигнал снятия блокировки микрофона
                if source_ws:
                    await async_send(source_ws, {"type": "new_message", "message_id": bot_message_id, "ui_msg_id": str(bot_message_id) if bot_message_id else None, "sender": "Бот", "text": "", "actions": []})

        # Сохранение текста в БД
        if dialog_id and bot_message_id:
            if final_text:
                await cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_text, bot_message_id))
                await conn.commit()
            else:
                await cursor.execute("DELETE FROM messages WHERE id = %s", (bot_message_id,))
                await conn.commit()

        logger.info(f"[DONE] Оркестратор завершил работу.\n" + "="*50)

    except Exception as e:
        logger.error(f"[ERROR в Мозге] {e}", exc_info=True)
        try:
            if cursor and bot_message_id:
                await cursor.execute("DELETE FROM messages WHERE id = %s", (bot_message_id,))
                if conn: await conn.commit()
            if source_ws and bot_message_id:
                await async_send(source_ws, {"type": "delete_message", "ui_msg_id": str(bot_message_id)})
        except: pass
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()