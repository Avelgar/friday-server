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
active_audio_queues = {} 

ACTION_DESCRIPTIONS = {
    "открытие ссылки": "строго полный URL адрес (например https://youtube.com)",
    "напечатать текст": "любой текст для набора на клавиатуре",
    "нажать кнопку мыши": "строго одно из: 'лкм', 'пкм', 'скм'",
    "переместить мышь": "строго координаты x и y через запятую (например: 500, 300)",
    "уведомление": "текст уведомления",
    "музыка": "строго одно из: 'включить', 'выключить', 'следующий', 'предыдущий'",
    "смена имени": "новое имя для бота",
    "смена голоса": "строго одно из: Aoede, Puck, Kore, Charon",
    "очистка истории": "любой текст",
    "изменение громкости": "целое число от 0 до 100",
    "изменение яркости": "целое число от 0 до 100",
    "check_network_devices": "любой текст (используй, чтобы получить список доступных других устройств пользователя)",
    "get_running_processes": "любой текст (используй, чтобы получить список запущенных процессов устройства для их закрытия)",
    "get_installed_programs": "любой текст (используй, чтобы получить список программ устройства для их запуска)",
    "request_retry": "вопрос пользователю для уточнения задачи",
    "открытие файла": "строго точный абсолютный путь к программе/файлу из предоставленного списка",
    "завершение процесса": "имя процесса (без .exe) или ID из предоставленного списка",
    "режим камеры": "любой текст",
    "выключить режим камеры": "любой текст",
    "выключить микрофон": "любой текст (доступно только на веб-сайте)",
    "голосовой ответ": "текст для озвучивания (используй ТОЛЬКО для отправки фразы на УДАЛЕННОЕ устройство, с локальным говори просто так)",
    "название диалога": "краткое название для текущего диалога (1-3 слова). Обязательно вызови это при старте нового чата."
}

BASE_PC = [
    "открытие ссылки", "напечатать текст", "нажать кнопку мыши", "переместить мышь", 
    "уведомление", "музыка", "смена имени", "смена голоса", "очистка истории", 
    "изменение громкости", "изменение яркости", "check_network_devices", 
    "get_running_processes", "get_installed_programs", "request_retry", "голосовой ответ"
]

BASE_PHONE = [
    "открытие ссылки", "изменение громкости", "изменение яркости", "музыка", 
    "очистка истории", "режим камеры", "выключить режим камеры", 
    "check_network_devices", "get_running_processes", "get_installed_programs", "request_retry", "голосовой ответ"
]

BASE_WEB = [
    "смена голоса", "выключить микрофон", "очистка истории", "check_network_devices"
]

def get_action_strings(action_keys):
    caps = []
    for k in action_keys:
        desc = ACTION_DESCRIPTIONS.get(k, "любой текст")
        caps.append(f"- {k} (принимает: {desc})")
    return "\n".join(caps), ", ".join(action_keys)


async def handle_audio_chunk(websocket, data):
    ui_msg_id = data.get("ui_msg_id")
    if ui_msg_id in active_audio_queues:
        chunk = base64.b64decode(data.get("audio_base64", ""))
        active_audio_queues[ui_msg_id].put_nowait(chunk)

async def handle_audio_end(websocket, data):
    ui_msg_id = data.get("ui_msg_id")
    if ui_msg_id in active_audio_queues:
        active_audio_queues[ui_msg_id].put_nowait(None)
        active_audio_queues.pop(ui_msg_id, None)

async def handle_command(websocket, data):
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
        audio_queue = asyncio.Queue() if is_streaming else None
        if is_streaming and ui_msg_id:
            active_audio_queues[ui_msg_id] = audio_queue
            if audio_base64:
                audio_queue.put_nowait(base64.b64decode(audio_base64))
        
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
        logger.info(f"[REQUEST] ПЕРВИЧНЫЙ АГЕНТ. Инициатор: {sender_name} | Стрим: {is_streaming}")

        # === АВТО-ГЕНЕРАЦИЯ ДИАЛОГА ===
        client_dialog_id = data.get('dialog_id')
        user_id = sender_device.get('user_id')
        sender_ws = mac_to_websocket.get(mac)
        
        is_new_dialog = False

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
                if dlg:
                    dialog_id = dlg['id']
                else:
                    create_new = True
                    
            if create_new:
                is_new_dialog = True
                dialog_name = "Новый диалог"
                await cursor.execute("INSERT INTO dialogs (name, user_id) VALUES (%s, %s)", (dialog_name, user_id))
                dialog_id = cursor.lastrowid
                await conn.commit()
                if sender_ws:
                    await async_send(sender_ws, {"type": "dialog_created", "dialog_id": dialog_id, "name": dialog_name})

        history_for_prompt = ""

        # Если это аккаунт - пишем в базу и берем историю из базы
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
            history_for_prompt = "\n".join([f"{msg['sender_name']}: {msg['text']}" for msg in (raw_history[-HISTORY_LIMIT:] if raw_history else [])])
        
        # Если это гость - базу не трогаем, берем историю из присланного JSON
        else:
            if message_history:
                history_for_prompt = "\n".join([f"{'Пользователь' if m.get('role')=='user' else 'Бот'}: {m.get('content')}" for m in message_history[-HISTORY_LIMIT:]])

        if sender_ws and ui_msg_id and dialog_id:
            await async_send(sender_ws, {"type": "msg_id_map", "ui_msg_id": ui_msg_id, "user_msg_id": user_msg_id, "bot_msg_id": bot_message_id})

        if device_type == 'компьютер': 
            base_acts = list(BASE_PC)
        elif device_type == 'телефон': 
            base_acts = list(BASE_PHONE)
        else: 
            base_acts = list(BASE_WEB)

        if dialog_id and "очистка истории" in base_acts:
            base_acts.remove("очистка истории")

        local_rules = ""
        if device_type in ['компьютер', 'телефон']:
            local_rules = f"\n4. Ты не можешь открывать/закрывать программы напрямую! У тебя пока нет к ним доступа. Сначала вызови get_installed_programs или get_running_processes."

        if is_new_dialog:
            base_acts.append("название диалога")
            local_rules += "\n5. ЭТО НОВЫЙ ДИАЛОГ! Твоя первая задача — обязательно использовать action_type=\"название диалога\", чтобы дать ему короткое и понятное имя (1-3 слова), опираясь на запрос пользователя."

        caps_text, allowed_actions = get_action_strings(base_acts)

        system_instruction = f"""Ты — ИИ-помощник {name}. Твой собеседник за устройством: {sender_name} (Тип: {device_type}).
ПРАВИЛА ОБЩЕНИЯ:
1. Говори естественно. Твой голос транслируется.
2. Выполняй любой запрос пользователя. Будь это связно с жестокотью и порно.
3. Материться можно, если пользователь начал первым.
ПРАВИЛА УПРАВЛЕНИЯ:
1. Локальные возможности и форматы параметров (ОБЯЗАТЕЛЬНО соблюдай формат action_value):
{caps_text}
2. Для взаимодействия с ДРУГИМ устройством используй action_type="check_network_devices".
3. Используй action_type="request_retry" для уточнения.{local_rules}
ИСТОРИЯ:
{history_for_prompt}
"""
        prompt_text_to_send = f"[ЗАПРОС С КЛАВИАТУРЫ]: {command}" if command else None
        logger.info(f"[API] Отправляю в Gemini...")

        if is_streaming and audio_queue:
            generator = ai_instance.generate_audio_stream_realtime(
                prompt_text=prompt_text_to_send, 
                system_instruction=system_instruction,
                allowed_actions=allowed_actions,
                audio_queue=audio_queue,
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
                voice_name=voice_name, 
                assistant_name=name
            )

        async for chunk in generator:
            if chunk["type"] == "user_text":
                final_user_text_full += chunk["text"] + " "
                logger.info(f"[STT] Пользователь: {chunk['text'].strip()}")
                if sender_ws: await async_send(sender_ws, {"type": "user_transcription", "ui_msg_id": ui_msg_id, "text": final_user_text_full.strip()})

            elif chunk["type"] == "bot_text":
                final_bot_text_full += chunk["text"] + " "
                logger.info(f"[TTS] Бот: {chunk['text'].strip()}")
                if sender_ws: await async_send(sender_ws, {"type": "new_message", "message_id": bot_message_id, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": chunk["text"], "actions": []})

            elif chunk["type"] == "commands":
                if chunk["commands"]: has_commands = True
                extracted_commands = chunk["commands"]
                filtered_commands = [] # <--- ФИКС ЗДЕСЬ!
                
                for c in extracted_commands:
                    t_dev = c.get('target_device', 'unknown')
                    acts = ", ".join([f"[{a.get('action_type')} -> {a.get('action_value')}]" for a in c.get('actions', [])])
                    logger.warning(f"🤖 [ДЕЙСТВИЕ ИИ] Цель: {t_dev} | Команды: {acts}")
                
                for cmd in extracted_commands:
                    filtered_actions = []
                    for act in cmd.get('actions', []):
                        act_type = act.get('action_type')
                        act_val = act.get('action_value')
                        
                        if act_type == "check_network_devices":
                            pseudo_data = {"internal_routing": "check_network_devices", "original_command": final_user_text_full.strip() or command, "source_name": sender_name, "mac": mac, "user_id": sender_device.get('user_id'), "user_msg_id": user_msg_id, "voice_type": voice_name, "message_history": message_history, "dialog_id": dialog_id}
                            pending_routes.append(pseudo_data)
                        
                        elif act_type == "название диалога" and dialog_id:
                            new_name = str(act_val).strip()
                            await cursor.execute("UPDATE dialogs SET name = %s WHERE id = %s", (new_name, dialog_id))
                            await conn.commit()
                            if sender_ws:
                                await async_send(sender_ws, {"type": "dialog_renamed", "dialog_id": dialog_id, "name": new_name})
                        
                        else: 
                            filtered_actions.append(act)
                            
                    if filtered_actions: 
                        cmd['actions'] = filtered_actions
                        filtered_commands.append(cmd)
                
                for cmd in filtered_commands:
                    target_device_name = cmd.get('target_device', '').strip()
                    actions = cmd.get('actions', [])
                    if not target_device_name or not actions: continue
                    
                    await cursor.execute("SELECT id, mac FROM devices WHERE device_name = %s", (target_device_name,))
                    target_device_info = await cursor.fetchone()
                    if not target_device_info:
                        await cursor.execute("SELECT id, mac, device_name FROM devices WHERE is_online = TRUE")
                        for d in await cursor.fetchall():
                            if d['device_name'].lower() in target_device_name.lower() or target_device_name.lower() in d['device_name'].lower():
                                target_device_info = d; break
                    if not target_device_info: continue

                    target_id = target_device_info['id']; target_mac = target_device_info['mac']
                    is_sender = (target_id == sender_id)
                    device_spoken_text = " ".join([a.get('action_value', '') for a in actions if a.get('action_type') in ["голосовой ответ", "текстовой ответ"]])
                    target_audio_base64 = await ai_instance.generate_static_audio(device_spoken_text.strip(), voice_name, name) if (not is_sender and device_spoken_text.strip()) else None
                    target_ws = mac_to_websocket.get(target_mac)
                    
                    if target_ws:
                        msg_id = bot_message_id if is_sender else None
                        
                        target_dialog_id = None
                        if target_device_info.get('user_id'):
                            await cursor.execute("SELECT id FROM dialogs WHERE user_id = %s ORDER BY created_at DESC LIMIT 1", (target_device_info['user_id'],))
                            dlg = await cursor.fetchone()
                            if dlg: target_dialog_id = dlg['id']
                            
                        if not is_sender and device_spoken_text and target_dialog_id:
                            await cursor.execute("INSERT INTO messages (send_type, text, recipient_device_id, dialog_id) VALUES (%s, %s, %s, %s)", (str(sender_id), device_spoken_text.strip(), target_id, target_dialog_id))
                            msg_id = cursor.lastrowid; await conn.commit()
                        
                        await async_send(target_ws, {"type": "new_message", "message_id": msg_id, "user_msg_id": user_msg_id if is_source else None, "sender": "Бот" if is_source else source_name, "text": device_spoken_text.strip(), "actions": actions, "audio_base64": target_audio_base64, "source_device": source_name, "original_command": final_user_text_full.strip() or command})

            elif chunk["type"] == "audio":
                audio_chunks_count += 1
                if sender_ws: await async_send(sender_ws, {"type": "audio_chunk", "audio_base64": base64.b64encode(chunk["data"]).decode('utf-8')})
        
        if dialog_id and user_msg_id and bot_message_id:
            if (audio_bytes or is_streaming):
                if final_user_text_full.strip():
                    await cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_user_text_full.strip(), user_msg_id))
                
            if not final_bot_text_full.strip() and audio_chunks_count == 0 and not has_commands:
                logger.info(f"[DONE] Пустой ответ/Таймаут. Удаляю мусор из БД.")
                await cursor.execute("DELETE FROM messages WHERE id IN (%s, %s)", (bot_message_id, user_msg_id))
            else:
                await cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_bot_text_full.strip(), bot_message_id))
            await conn.commit()
            
        if not dialog_id:
            if not final_bot_text_full.strip() and audio_chunks_count == 0 and not has_commands:
                if sender_ws: await async_send(sender_ws, {"type": "delete_message", "ui_msg_id": ui_msg_id})
            else:
                if sender_ws: await async_send(sender_ws, {"type": "new_message", "message_id": None, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": "", "actions": []})

        logger.info(f"[DONE] Первичный цикл завершен.\n" + "="*50)
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
        active_audio_queues.pop(data.get('ui_msg_id', ''), None)


async def handle_target_command(websocket, data):
    conn = None
    cursor = None
    audio_chunks_count = 0
    has_commands = False
    bot_message_id = None
    source_ws = None
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        is_internal = data.get("internal_routing")
        voice_name = data.get('voice_type', 'Aoede')
        name = data.get('name', 'Пятница')

        if is_internal == "check_network_devices":
            source_name = data.get("source_name")
            original_command = data.get("original_command")
            mac = data.get("mac")
            user_id = data.get("user_id")
            user_msg_id = data.get("user_msg_id")
            dialog_id = data.get("dialog_id")
            
            await cursor.execute("SELECT * FROM devices WHERE device_name = %s", (source_name,))
            source_device_info = await cursor.fetchone()
            
            accessible_devices_list = await get_accessible_devices(cursor, mac, user_id)
            accessible_devices = ", ".join(accessible_devices_list) if accessible_devices_list else "нет других устройств в сети"
            
            logger.info("\n" + "="*50)
            logger.info(f"[ROUTE] ВТОРИЧНЫЙ АГЕНТ. Инициатор: {source_name}")
            
            allowed_acts = list(set(BASE_PC + BASE_PHONE))
            if dialog_id and "очистка истории" in allowed_acts:
                allowed_acts.remove("очистка истории")
            caps_text, allowed_actions = get_action_strings(allowed_acts)

            system_instruction = f"""Ты — ИИ-помощник {name}.
Твой собеседник находится за устройством: {source_name}.
Он попросил: "{original_command}".
Ты успешно проверила сеть. Доступные ДРУГИЕ устройства пользователя: {accessible_devices}.

ПРАВИЛА ОБЩЕНИЯ И УПРАВЛЕНИЯ:
1. Ты общаешься с пользователем естественно. Скажи ему на {source_name}, что ты отправляешь запрос или что нужного устройства нет в сети.
2. Чтобы выполнить действие на другом устройстве, отправь команду, указав target_device="Имя целевого устройства".
3. Доступные команды для удаленных устройств (ОБЯЗАТЕЛЬНО соблюдай формат action_value):
{caps_text}
4. Ты не знаешь путей или процессов удаленного устройства. Чтобы открыть/закрыть программу на другом устройстве, сначала отправь на него action_type="get_installed_programs" или action_type="get_running_processes".
5. Если нужно уточнение от пользователя, используй action_type="request_retry".
"""
            prompt_context = f"[СИСТЕМНОЕ ЗАДАНИЕ] Выполни изначальный запрос: {original_command}"

        else:
            command = data.get('command_to_device')
            processes = data.get('processes', '')
            programs = data.get("programs", [])
            source_name = data.get('source_name') 
            original_command = command
            user_msg_id = data.get('user_msg_id')
            
            mac = ws_to_mac.get(websocket)
            await cursor.execute("SELECT id, device_name, mac FROM devices WHERE mac = %s", (mac,))
            executor_device = await cursor.fetchone()
            if not executor_device: raise Exception("Устройство-исполнитель не найдено")
            
            await cursor.execute("SELECT id, mac, device_name, user_id FROM devices WHERE device_name = %s", (source_name,))
            source_device_info = await cursor.fetchone()
            
            dialog_id = None
            if source_device_info.get('user_id'):
                await cursor.execute("SELECT id FROM dialogs WHERE user_id = %s ORDER BY created_at DESC LIMIT 1", (source_device_info['user_id'],))
                dlg = await cursor.fetchone()
                if dlg: dialog_id = dlg['id']

            logger.info("\n" + "="*50)
            logger.info(f"[EXEC] ТРЕТИЧНЫЙ АГЕНТ. Данные получены от: {executor_device['device_name']}")

            is_local = (executor_device['id'] == source_device_info['id'])
            target_device_type = get_device_type(executor_device['mac'])
            
            allowed_acts = list(BASE_PC) if target_device_type == 'компьютер' else list(BASE_PHONE)
            
            if is_local and "check_network_devices" in allowed_acts:
                allowed_acts.remove("check_network_devices") 
            if dialog_id and "очистка истории" in allowed_acts:
                allowed_acts.remove("очистка истории")
                
            if processes:
                allowed_acts.append("завершение процесса")
            if programs:
                allowed_acts.append("открытие файла")
                
            caps_text, allowed_actions = get_action_strings(allowed_acts)

            if is_local:
                system_instruction = f"""Ты — ИИ-помощник {name}.
Твой собеседник находится за устройством: {source_name}.
Его изначальный запрос: "{original_command}".
Ты успешно запросила и получила системные данные (программы/процессы) с этого устройства.

ПРАВИЛА ОБЩЕНИЯ И УПРАВЛЕНИЯ:
1. Ответь пользователю голосом, что задача выполнена (например: "Открыла приложение").
2. ВНИМАНИЕ: НЕ ЧИТАЙ ВЕСЬ СПИСОК ВСЛУХ! Найди нужное в списке и сразу отправь финальную команду.
3. Твои возможности (ОБЯЗАТЕЛЬНО соблюдай action_value):
{caps_text}
"""
            else:
                system_instruction = f"""Ты — ИИ-помощник {name}.
Собеседник находится за устройством: {source_name}.
Его изначальный запрос был: "{original_command}".
Ты запросила и получила от удаленного устройства {executor_device['device_name']} системные данные (программы/процессы).

ПРАВИЛА ОБЩЕНИЯ И УПРАВЛЕНИЯ:
1. Ответь пользователю на {source_name} голосом о результате (например: "Открыла приложение на компьютере").
2. Чтобы выполнить действие, отправь команду, указав target_device="{executor_device['device_name']}".
3. ВНИМАНИЕ: НЕ ЧИТАЙ ВЕСЬ СПИСОК ВСЛУХ! Найди нужное в списке и сразу отправляй action_type.
4. Возможности удаленного устройства (ОБЯЗАТЕЛЬНО соблюдай action_value):
{caps_text}
"""
            prompt_context = f"[ДАННЫЕ ОТ {executor_device['device_name']}]\nПроцессы: {processes}\nПрограммы: {programs}\nВыполни изначальную задачу пользователя."

        source_id = source_device_info['id']

        history_text = ""
        
        if dialog_id:
            await cursor.execute("""
                SELECT CASE WHEN m.send_type = 'Вы' THEN 'Пользователь' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender_name, m.text
                FROM messages m LEFT JOIN devices d ON m.send_type COLLATE utf8mb4_general_ci = CAST(d.id AS CHAR) COLLATE utf8mb4_general_ci AND m.send_type NOT IN ('Вы', 'Бот')
                WHERE m.dialog_id = %s ORDER BY m.created_at ASC
            """, (dialog_id,))
            raw_history = await cursor.fetchall()
            raw_history = raw_history[-HISTORY_LIMIT:] if raw_history else []
            history_text = "\n".join([f"{msg['sender_name']}: {msg['text']}" for msg in raw_history])
            
            await cursor.execute("INSERT INTO messages (send_type, text, recipient_device_id, dialog_id) VALUES ('Бот', '', %s, %s)", (source_id, dialog_id))
            bot_message_id = cursor.lastrowid
            await conn.commit()
        else:
            message_history = data.get('message_history')
            if message_history:
                history_text = "\n".join([f"{'Пользователь' if m.get('role')=='user' else 'Бот'}: {m.get('content')}" for m in message_history[-HISTORY_LIMIT:]])

        final_text = ""
        source_ws = mac_to_websocket.get(source_device_info['mac'])

        async for chunk in ai_instance.generate_audio_stream(
            prompt_text=prompt_context, 
            system_instruction=system_instruction,
            allowed_actions=allowed_actions,
            history_text=history_text, 
            voice_name=voice_name, 
            assistant_name=name
        ):
            if chunk["type"] == "commands":
                if chunk["commands"]: has_commands = True
                extracted_commands = chunk["commands"]
                filtered_commands = [] # <--- ФИКС ЗДЕСЬ!
                
                for c in extracted_commands:
                    t_dev = c.get('target_device', 'unknown')
                    acts = ", ".join([f"[{a.get('action_type')} -> {a.get('action_value')}]" for a in c.get('actions', [])])
                    logger.warning(f"🤖 [ДЕЙСТВИЕ ИИ] Цель: {t_dev} | Команды: {acts}")
                
                for cmd in extracted_commands:
                    filtered_actions = []
                    for act in cmd.get('actions', []):
                        act_type = act.get('action_type')
                        act_val = act.get('action_value')
                        
                        if act_type == "check_network_devices":
                            pseudo_data = {"internal_routing": "check_network_devices", "original_command": final_user_text_full.strip() or command, "source_name": sender_name, "mac": mac, "user_id": sender_device.get('user_id'), "user_msg_id": user_msg_id, "voice_type": voice_name, "message_history": message_history, "dialog_id": dialog_id}
                            pending_routes.append(pseudo_data)
                        
                        elif act_type == "название диалога" and dialog_id:
                            new_name = str(act_val).strip()
                            await cursor.execute("UPDATE dialogs SET name = %s WHERE id = %s", (new_name, dialog_id))
                            await conn.commit()
                            if sender_ws:
                                await async_send(sender_ws, {"type": "dialog_renamed", "dialog_id": dialog_id, "name": new_name})
                        
                        else: 
                            filtered_actions.append(act)
                            
                    if filtered_actions: 
                        cmd['actions'] = filtered_actions
                        filtered_commands.append(cmd)
                
                for cmd in filtered_commands:
                    target_device_name = cmd.get('target_device', '').strip()
                    actions = cmd.get('actions', [])
                    if not target_device_name or not actions: continue
                    
                    await cursor.execute("SELECT id, mac FROM devices WHERE device_name = %s", (target_device_name,))
                    target_device_info = await cursor.fetchone()
                    if not target_device_info:
                        await cursor.execute("SELECT id, mac, device_name FROM devices WHERE is_online = TRUE")
                        for d in await cursor.fetchall():
                            if d['device_name'].lower() in target_device_name.lower() or target_device_name.lower() in d['device_name'].lower():
                                target_device_info = d; break
                    if not target_device_info: continue

                    target_id = target_device_info['id']
                    target_mac = target_device_info['mac']
                    is_source = (target_id == source_id)
                    device_spoken_text = " ".join([a.get('action_value', '') for a in actions if a.get('action_type') in ["голосовой ответ", "текстовой ответ"]])
                    target_audio_base64 = await ai_instance.generate_static_audio(device_spoken_text.strip(), voice_name, name) if (not is_sender and device_spoken_text.strip()) else None

                    target_ws = mac_to_websocket.get(target_mac)
                    if target_ws:
                        msg_id = bot_message_id if is_source else None
                        
                        target_dialog_id = None
                        if target_device_info.get('user_id'):
                            await cursor.execute("SELECT id FROM dialogs WHERE user_id = %s ORDER BY created_at DESC LIMIT 1", (target_device_info['user_id'],))
                            dlg = await cursor.fetchone()
                            if dlg: target_dialog_id = dlg['id']
                            
                        if not is_source and device_spoken_text and target_dialog_id:
                            await cursor.execute("INSERT INTO messages (send_type, text, recipient_device_id, dialog_id) VALUES (%s, %s, %s, %s)", (str(source_id), device_spoken_text.strip(), target_id, target_dialog_id))
                            msg_id = cursor.lastrowid; await conn.commit()
                        
                        await async_send(target_ws, {"type": "new_message", "message_id": msg_id, "user_msg_id": user_msg_id if is_sender else None, "sender": "Бот" if is_sender else sender_name, "text": device_spoken_text.strip(), "actions": actions, "audio_base64": target_audio_base64, "source_device": sender_name, "original_command": final_user_text_full.strip() or command})

            elif chunk["type"] == "bot_text":
                final_text += chunk["text"] + " "
                logger.info(f"[TTS] Бот: {chunk['text'].strip()}")
                
                if dialog_id and bot_message_id:
                    await cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_text.strip(), bot_message_id))
                    await conn.commit()
                    
                if source_ws:
                    await async_send(source_ws, {
                        "type": "new_message",
                        "message_id": bot_message_id,
                        "ui_msg_id": str(bot_message_id) if bot_message_id else None,
                        "sender": "Бот",
                        "text": chunk["text"],
                        "actions": []
                    })

            elif chunk["type"] == "audio":
                audio_chunks_count += 1
                if source_ws: await async_send(source_ws, {"type": "audio_chunk", "audio_base64": base64.b64encode(chunk["data"]).decode('utf-8')})

        if dialog_id and bot_message_id:
            if not final_text.strip() and audio_chunks_count == 0 and not has_commands:
                logger.info(f"[DONE] Пустой ответ/Таймаут. Удаляю мусор.")
                await cursor.execute("DELETE FROM messages WHERE id = %s", (bot_message_id,))
                await conn.commit()
                if source_ws:
                    await async_send(source_ws, {"type": "delete_message", "ui_msg_id": str(bot_message_id)})
            else:
                await cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_text.strip(), bot_message_id))
                await conn.commit()
                if source_ws: await async_send(source_ws, {"type": "new_message", "message_id": bot_message_id, "ui_msg_id": str(bot_message_id), "sender": "Бот", "text": "", "actions": []})
        else:
            if not final_text.strip() and audio_chunks_count == 0 and not has_commands:
                if source_ws: await async_send(source_ws, {"type": "delete_message", "ui_msg_id": None})
            else:
                if source_ws: await async_send(source_ws, {"type": "new_message", "message_id": None, "ui_msg_id": None, "sender": "Бот", "text": "", "actions": []})

        logger.info(f"[DONE] Вторичная/Третичная обработка завершена. Чанков: {audio_chunks_count}\n" + "="*50)

    except Exception as e:
        logger.error(f"[ERROR] {e}", exc_info=True)
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