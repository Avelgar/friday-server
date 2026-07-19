import sys
import asyncio
import websockets
import json
import base64
import logging
import time
import hashlib
import jwt
import re  # ДОБАВЛЕН ИМПОРТ RE
from datetime import datetime

# Добавляем корневую директорию
sys.path.append('/opt/friday')

from app.config.settings import JWT_SECRET
from app.database.connection import get_db_connection
from app.services.ai_service import ai_instance

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("WS_Server")

active_connections = {}  
id_to_websocket = {}     
last_ping_times = {}     
PING_TIMEOUT = 70

def assemble_legacy_actions_string(response_text, extracted_commands, command_type):
    """
    Собирает из раздельного ответа Gemini классическую строку действий вида:
    'голосовой ответ|Текст ответа⸵открытие ссылки|https://ya.ru'
    Это обеспечивает 100% совместимость со старой базой данных и клиентами C# / Java.
    """
    # Определяем базовый тип ответа в чат
    base_type = "голосовой ответ" if command_type == "голосовое сообщение" else "текстовой ответ"
    
    # Если Gemini вернула пустой текст, ставим дефолтный (или берем из субтитров, если она их сгенерировала)
    clean_response_text = response_text.strip() if response_text else "Выполняю"
    
    legacy_parts = [f"{base_type}|{clean_response_text}"]
    
    # Добавляем все фоновые команды через разделитель ⸵
    for cmd in extracted_commands:
        actions = cmd.get('actions', [])
        for act in actions:
            a_type = act.get('action_type', '').strip()
            a_value = act.get('action_value', '').strip()
            
            # Игнорируем дублирующие текстовые ответы/субтитры в командах устройства
            if a_type in ["субтитры", "голосовой ответ", "текстовой ответ"]:
                continue
                
            legacy_parts.append(f"{a_type}|{a_value}")
            
    return "⸵".join(legacy_parts)
    
def get_device_type(mac):
    if not mac: return "неизвестно"
    if '-' in mac: return "компьютер"
    if 'WEB' in mac: return "браузер"
    if mac == "b8:27:eb:00:51:06": return "распберри"
    return "телефон"

def get_accessible_devices(cursor, current_mac, user_id):
    devices = {}
    
    # 1. По аккаунту (user_id)
    if user_id:
        cursor.execute("SELECT mac, device_name FROM devices WHERE user_id = %s AND websocket_id IS NOT NULL AND mac != %s", (user_id, current_mac))
        for row in cursor.fetchall():
            devices[row['mac']] = f"{row['device_name']} ({get_device_type(row['mac'])})"
            
    # 2. Прямой и обратный access_list
    cursor.execute("SELECT mac, device_name, access_list FROM devices WHERE websocket_id IS NOT NULL AND mac != %s", (current_mac,))
    for row in cursor.fetchall():
        target_mac = row['mac']
        target_al = row.get('access_list') or ''
        
        # Если текущий MAC есть в access_list ДРУГОГО устройства (Обратная связь)
        if current_mac in target_al:
            devices[target_mac] = f"{row['device_name']} ({get_device_type(target_mac)})"
            
        # Заодно ищем прямой access_list для текущего устройства (вытягиваем его из БД, если еще не подгружен)
        if target_mac == current_mac:
            pass # Игнорируем себя
            
    # 3. Прямой access_list текущего устройства
    cursor.execute("SELECT access_list FROM devices WHERE mac = %s", (current_mac,))
    my_al_row = cursor.fetchone()
    if my_al_row and my_al_row.get('access_list'):
        my_macs = [m.strip() for m in my_al_row['access_list'].split(';') if m.strip()]
        if my_macs:
            placeholders = ','.join(['%s']*len(my_macs))
            cursor.execute(f"SELECT mac, device_name FROM devices WHERE mac IN ({placeholders}) AND websocket_id IS NOT NULL AND mac != %s", tuple(my_macs) + (current_mac,))
            for row in cursor.fetchall():
                devices[row['mac']] = f"{row['device_name']} ({get_device_type(row['mac'])})"

    return list(devices.values())

async def async_send(websocket, data):
    try:
        json_data = json.dumps(data, ensure_ascii=False)
        encoded_data = base64.b64encode(json_data.encode('utf-8')).decode('utf-8')
        await websocket.send(encoded_data)
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения: {e}")

async def handle_web_client_auth(websocket, data):
    conn = None
    cursor = None
    audio_chunks_count = 0
    try:
        token = data.get('token')
        login = data.get('login')
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            user_id = payload['user_id']
        except:
            await async_send(websocket, {"status": "error", "message": "Invalid token"})
            await websocket.close()
            return
        
        mac_hash = hashlib.md5(str(token).encode()).hexdigest()[:13]
        mac = f"WEB{mac_hash}"
        device_name = f"Браузер {login} {mac}"
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        websocket_id = id(websocket)
        
        cursor.execute("SELECT * FROM devices WHERE mac = %s", (mac,))
        device = cursor.fetchone()
        
        if device:
            cursor.execute("UPDATE devices SET websocket_id = %s, device_name = %s WHERE mac = %s", (websocket_id, device_name, mac))
        else:
            cursor.execute("INSERT INTO devices (mac, device_name, password, access_list, websocket_id, user_id) VALUES (%s, %s, '123', '', %s, %s)", (mac, device_name, websocket_id, user_id))
        
        cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
        device_id = cursor.fetchone()['id'] if cursor.rowcount > 0 else None
        
        history =[]
        if device_id:
            cursor.execute("""
                SELECT m.id, CASE WHEN m.send_type = 'Вы' THEN 'Вы' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender, m.text, m.time
                FROM messages m LEFT JOIN devices d ON m.send_type = CAST(d.id AS CHAR) AND m.send_type != 'Вы' AND m.send_type != 'Бот'
                WHERE m.recipient_device_id = %s ORDER BY m.time ASC
            """, (device_id,))
            for msg in cursor.fetchall():
                history.append({"id": msg['id'], "sender": msg['sender'], "text": msg['text'], "time": msg['time'].strftime('%Y-%m-%d %H:%M:%S')})
        
        conn.commit()
        await async_send(websocket, {"status": "success", "message": "Данные успешно обработаны!", "history": history})
        
    except Exception as e:
        await async_send(websocket, {"status": "error", "message": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

async def handle_command(websocket, data):
    conn = None
    cursor = None
    user_msg_id = None
    bot_message_id = None
    audio_chunks_count = 0  # СЮДА ДОБАВИЛИ!

    final_user_text_full = ""
    final_bot_text_full = ""

    try:
        command = data.get('command', '[Пользователь отправил аудиосообщение, прослушай его]')
        timestamp_str = data.get('timestamp')
        name = data.get('name', 'Пятница')
        voice_name = data.get('voice_type', 'Aoede')
        screenshot_base64 = data.get('screenshot')
        audio_base64 = data.get('audio_base64') 
        mac = data.get('mac')
        ui_msg_id = data.get('ui_msg_id') # <--- СЧИТЫВАЕМ CLIENT GUID!
        
        image_bytes = base64.b64decode(screenshot_base64) if screenshot_base64 else None
        audio_bytes = base64.b64decode(audio_base64) if audio_base64 else None

        fixed_timestamp_str = timestamp_str
        if timestamp_str and '.' in timestamp_str and '+' in timestamp_str:
            try:
                m, t = timestamp_str.split('+')
                if '.' in m:
                    s, f = m.split('.')
                    fixed_timestamp_str = f"{s}.{f[:6]}+{t}"
            except: pass
    
        mysql_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        
        cursor.execute("SELECT * FROM devices WHERE websocket_id = %s", (id(websocket),))
        sender_device = cursor.fetchone()
        if not sender_device and mac:
            cursor.execute("SELECT * FROM devices WHERE mac = %s", (mac,))
            sender_device = cursor.fetchone()
        if not sender_device: raise Exception("Устройство отправителя не найдено")
        
        sender_id = sender_device['id']
        sender_name = sender_device['device_name']

        # В БД сохраняем пустую плашку, обновим по мере прихода транскрипций
        db_user_placeholder = "🎤 [Слушаю...]" if audio_bytes else (command if command else "🖼️ [Фото]")
        db_bot_placeholder = "" 
        
        logger.info("\n" + "="*50)
        logger.info(f"[REQUEST] ПЕРВИЧНЫЙ ЦИКЛ. Инициатор: {sender_name}")
        logger.info(f"[INFO] Запрос: {db_user_placeholder}")

        cursor.execute("INSERT INTO messages (send_type, text, time, recipient_device_id) VALUES ('Вы', %s, %s, %s)", (db_user_placeholder, mysql_time, sender_id))
        conn.commit()
        user_msg_id = cursor.lastrowid

        cursor.execute("INSERT INTO messages (send_type, text, time, recipient_device_id) VALUES ('Бот', %s, %s, %s)", (db_bot_placeholder, mysql_time, sender_id))
        conn.commit()
        bot_message_id = cursor.lastrowid

        history_for_prompt = "" 
        cursor.execute("""
            SELECT CASE WHEN m.send_type = 'Вы' THEN 'Пользователь' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender_name, m.text
            FROM messages m LEFT JOIN devices d ON m.send_type = CAST(d.id AS CHAR) AND m.send_type NOT IN ('Вы', 'Бот')
            WHERE m.recipient_device_id = %s AND m.id < %s ORDER BY m.time ASC
        """, (sender_id, user_msg_id))
        history_for_prompt = "\n".join([f"{msg['sender_name']}: {msg['text']}" for msg in cursor.fetchall()])

        device_type = get_device_type(mac)
        accessible_devices_list = get_accessible_devices(cursor, mac, sender_device.get('user_id'))
        accessible_devices = ", ".join(accessible_devices_list) if accessible_devices_list else "нет доступных устройств"

        prompt = f"""[СИСТЕМНЫЕ ДАННЫЕ]
Текущее время: {fixed_timestamp_str}
Устройство отправителя: {sender_name} (Тип: {device_type}).
Доступные устройства: {accessible_devices}.

[ЗАПРОС ПОЛЬЗОВАТЕЛЯ]:
{command}"""
        
        logger.info(f"[API] Отправляю в Gemini Live API (Audio: {bool(audio_bytes)}, Image: {bool(image_bytes)})...")

        async for chunk in ai_instance.generate_audio_stream(
            prompt_text=prompt, 
            audio_bytes=audio_bytes,
            image_bytes=image_bytes, 
            history_text=history_for_prompt, 
            voice_name=voice_name, 
            assistant_name=name
        ):
            # 1. ТРАНСКРИПЦИЯ ГОЛОСА ПОЛЬЗОВАТЕЛЯ
            if chunk["type"] == "user_text":
                final_user_text_full += chunk["text"] + " "
                logger.info(f"[STT] Пользователь: {final_user_text_full}")
                cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_user_text_full.strip(), user_msg_id))
                conn.commit()
                
                # Шлем на клиент, чтобы обновить UI (Передаем оригинальный ui_msg_id!)
                if sender_device['websocket_id']:
                    sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
                    if sender_ws:
                        await async_send(sender_ws, {
                            "type": "user_transcription",
                            "ui_msg_id": ui_msg_id, # Передаем client GUID!
                            "text": final_user_text_full.strip()
                        })

            # 2. ТРАНСКРИПЦИЯ ОТВЕТА БОТА
            elif chunk["type"] == "bot_text":
                final_bot_text_full += chunk["text"] + " "
                logger.info(f"[TTS] Бот: {chunk['text']}")
                cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_bot_text_full.strip(), bot_message_id))
                conn.commit()
                
                # Шлем на клиент, чтобы дописать текст в баббл
                if sender_device['websocket_id']:
                    sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
                    if sender_ws:
                        await async_send(sender_ws, {
                            "type": "new_message",
                            "message_id": bot_message_id,
                            "ui_msg_id": ui_msg_id, # client GUID
                            "sender": "Бот",
                            "text": chunk["text"],
                            "actions": []
                        })

            # 3. ВЫЗОВ ИНСТРУМЕНТОВ
            elif chunk["type"] == "commands":
                extracted_commands = chunk["commands"]
                logger.info(f"[JSON] Команды: {extracted_commands}")
                
                for cmd in extracted_commands:
                    target_device_name = cmd.get('target_device', '').strip()
                    actions = cmd.get('actions', [])
                    if not target_device_name or not actions: continue
                    
                    cursor.execute("SELECT * FROM devices WHERE device_name = %s", (target_device_name,))
                    target_device_info = cursor.fetchone()
                    if not target_device_info:
                        cursor.execute("SELECT * FROM devices WHERE websocket_id IS NOT NULL")
                        for d in cursor.fetchall():
                            if d['device_name'].lower() in target_device_name.lower() or target_device_name.lower() in d['device_name'].lower():
                                target_device_info = d
                                break
                    if not target_device_info: continue

                    target_id = target_device_info['id']
                    is_sender = (target_id == sender_id)

                    device_spoken_text = ""
                    for a in actions:
                        if a.get('action_type') == "голосовой ответ":
                            device_spoken_text += a.get('action_value', '') + " "
                    device_spoken_text = device_spoken_text.strip()

                    target_audio_base64 = None
                    if not is_sender and device_spoken_text:
                        target_audio_base64 = await ai_instance.generate_static_audio(device_spoken_text, voice_name, name)

                    if target_device_info['websocket_id']:
                        target_ws = id_to_websocket.get(int(target_device_info['websocket_id']))
                        if target_ws:
                            msg_id = bot_message_id if is_sender else None
                            if not is_sender:
                                cursor.execute("INSERT INTO messages (send_type, text, time, recipient_device_id) VALUES (%s, %s, %s, %s)", 
                                             (str(sender_id), device_spoken_text, mysql_time, target_id))
                                msg_id = cursor.lastrowid
                                conn.commit()
                            
                            await async_send(target_ws, {
                                "type": "new_message",
                                "message_id": msg_id,
                                "ui_msg_id": ui_msg_id, # client GUID
                                "sender": "Бот" if is_sender else sender_name,
                                "text": device_spoken_text, 
                                "actions": actions,
                                "audio_base64": target_audio_base64,
                                "source_device": sender_name,
                                "original_command": final_user_text_full.strip()
                            })

            # 4. ПОТОКОВЫЙ ЗВУК БОТА
            elif chunk["type"] == "audio":
                audio_chunks_count += 1
                if sender_device['websocket_id']:
                    sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
                    if sender_ws:
                        await async_send(sender_ws, {
                            "type": "audio_chunk",
                            "audio_base64": base64.b64encode(chunk["data"]).decode('utf-8')
                        })
        
        # Финальная проверка, если Гугл молчал
        if not final_bot_text_full.strip() and audio_chunks_count == 0:
            cursor.execute("DELETE FROM messages WHERE id = %s", (bot_message_id,))
            conn.commit()

        # В самом конце функции обновляем или удаляем пустые сообщения
        if final_bot_text_full.strip():
            cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_bot_text_full.strip(), bot_message_id))
        else:
            # Если бот ничего не сказал, мы не удаляем запись, а пишем пустоту, чтобы разблокировать клиент!
            cursor.execute("UPDATE messages SET text = %s WHERE id = %s", ("", bot_message_id))
        conn.commit()

        # ВСЕГДА отправляем финальный new_message для разблокировки микрофона на клиенте!
        if sender_device['websocket_id']:
            sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
            if sender_ws:
                await async_send(sender_ws, {
                    "type": "new_message",
                    "message_id": bot_message_id,
                    "ui_msg_id": ui_msg_id,
                    "sender": "Бот",
                    "text": "", # Пусто, так как текст уже улетел в процессе стрима
                    "actions": []
                })

        logger.info(f"[DONE] Первичная обработка завершена. Чанков: {audio_chunks_count}\n" + "="*50)

    except Exception as e:
        logger.error(f"[ERROR] {e}", exc_info=True)
        
        # В случае критической ошибки на сервере — все равно пинаем клиент, чтобы он разблокировал микрофон!
        try:
            if sender_device and sender_device['websocket_id']:
                sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
                if sender_ws:
                    await async_send(sender_ws, {
                        "type": "new_message",
                        "message_id": None,
                        "ui_msg_id": ui_msg_id,
                        "sender": "Бот",
                        "text": "[Ошибка сервера]",
                        "actions": []
                    })
        except: pass


async def handle_target_command(websocket, data):
    conn = None
    cursor = None
    audio_chunks_count = 0
    try:
        command = data.get('command_to_device')
        processes = data.get('processes', '')
        programs = data.get("programs", [])
        name = data.get('name', 'Пятница')
        source_name = data.get('source_name') 
        user_msg_id = data.get('user_msg_id')
        voice_name = "Aoede" 
        
        mysql_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        
        cursor.execute("SELECT * FROM devices WHERE websocket_id = %s", (str(id(websocket)),))
        sender_device = cursor.fetchone()
        if not sender_device: raise Exception("Устройство отправителя данных не найдено")
        
        cursor.execute("SELECT * FROM devices WHERE device_name = %s", (source_name,))
        source_device_info = cursor.fetchone()
        if not source_device_info: raise Exception("Инициатор не найден")
        
        source_id = source_device_info['id']
        source_device_type = get_device_type(source_device_info.get('mac'))

        logger.info("\n" + "="*50)
        logger.info(f"[REQUEST] ВТОРИЧНЫЙ ЦИКЛ. Инициатор: {source_name}")
        
        cursor.execute("""
            SELECT CASE WHEN m.send_type = 'Вы' THEN 'Пользователь' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender_name, m.text
            FROM messages m LEFT JOIN devices d ON m.send_type = CAST(d.id AS CHAR) AND m.send_type NOT IN ('Вы', 'Бот')
            WHERE m.recipient_device_id = %s ORDER BY m.time ASC
        """, (source_id,))
        history_text = "\n".join([f"{msg['sender_name']}: {msg['text']}" for msg in cursor.fetchall()])
        
        accessible_devices = []
        if source_device_info.get('user_id'):
            cursor.execute("SELECT mac, device_name FROM devices WHERE user_id = %s AND websocket_id IS NOT NULL", (source_device_info['user_id'],))
            for dev in cursor.fetchall():
                accessible_devices.append(f"{dev['device_name']} ({get_device_type(dev['mac'])})")
        
        prompt_context = f"""[СИСТЕМНЫЕ ДАННЫЕ]
Устройство инициатора: "{source_name}" (Тип: {source_device_type})
Доступные устройства: {', '.join(accessible_devices) if accessible_devices else 'нет'}

С устройства {source_name} на устройство {sender_device['device_name']} был произведен запрос данных после команды "{command}". 
После чего устройство {sender_device['device_name']} вызвало тебя и передало эти данные:
Пути к программам: {programs}
Запущенные процессы: {processes}

Исходя из команды реши куда что отправить и какие действия выполнить."""
        
        cursor.execute("INSERT INTO messages (send_type, text, time, recipient_device_id) VALUES ('Бот', '', %s, %s)", (mysql_time, source_id))
        bot_message_id = cursor.lastrowid
        conn.commit()

        final_text = ""

        # В ГЕНЕРАТОР ПЕРЕДАЕМ ТОЛЬКО ТЕКСТ
        async for chunk in ai_instance.generate_audio_stream(
            prompt_text=prompt_context, 
            history_text=history_text, 
            voice_name=voice_name, 
            assistant_name=name
        ):
            if chunk["type"] == "commands":
                extracted_commands = chunk["commands"]
                spoken_text = chunk["text"].strip()
                final_text += spoken_text + " "
                
                source_commands_found = False
                for cmd in extracted_commands:
                    if cmd.get('target_device') == source_name:
                        source_commands_found = True
                        break
                
                if not source_commands_found and spoken_text:
                    extracted_commands.append({
                        'target_device': source_name,
                        'actions': [{'action_type': 'голосовой ответ', 'action_value': spoken_text}]
                    })
                
                for cmd in extracted_commands:
                    target_device_name = cmd.get('target_device', '').strip()
                    actions = cmd.get('actions', [])
                    if not target_device_name or not actions: continue
                    
                    cursor.execute("SELECT * FROM devices WHERE device_name = %s", (target_device_name,))
                    target_device_info = cursor.fetchone()
                    if not target_device_info:
                        cursor.execute("SELECT * FROM devices WHERE websocket_id IS NOT NULL")
                        for d in cursor.fetchall():
                            if d['device_name'].lower() in target_device_name.lower() or target_device_name.lower() in d['device_name'].lower():
                                target_device_info = d
                                break
                    if not target_device_info: continue

                    target_id = target_device_info['id']
                    is_source = (target_id == source_id)
                    
                    device_spoken_text = ""
                    for a in actions:
                        if a.get('action_type') in ["голосовой ответ", "текстовой ответ", "субтитры"]:
                            device_spoken_text += a.get('action_value', '') + " "
                    device_spoken_text = device_spoken_text.strip()

                    target_audio_base64 = None
                    if not is_source and device_spoken_text:
                        target_audio_base64 = await ai_instance.generate_static_audio(device_spoken_text, voice_name, name)

                    if target_device_info['websocket_id']:
                        target_ws = id_to_websocket.get(int(target_device_info['websocket_id']))
                        if target_ws:
                            msg_id = bot_message_id if is_source else None
                            
                            if not is_source and device_spoken_text:
                                cursor.execute("INSERT INTO messages (send_type, text, time, recipient_device_id) VALUES (%s, %s, %s, %s)", 
                                             (str(source_id), device_spoken_text, mysql_time, target_id))
                                msg_id = cursor.lastrowid
                                conn.commit()
                            
                            await async_send(target_ws, {
                                "type": "new_message",
                                "message_id": msg_id,
                                "user_msg_id": user_msg_id if is_source else None,
                                "sender": "Бот" if is_source else source_name,
                                "text": device_spoken_text, 
                                "actions": actions,
                                "audio_base64": target_audio_base64,
                                "source_device": source_name,
                                "original_command": command
                            })

            elif chunk["type"] == "audio":
                audio_chunks_count += 1
                if source_device_info['websocket_id']:
                    source_ws = id_to_websocket.get(int(source_device_info['websocket_id']))
                    if source_ws:
                        await async_send(source_ws, {
                            "type": "audio_chunk",
                            "audio_base64": base64.b64encode(chunk["data"]).decode('utf-8')
                        })

        if final_text.strip():
            cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_text.strip(), bot_message_id))
        else:
            cursor.execute("DELETE FROM messages WHERE id = %s", (bot_message_id,))
        conn.commit()

        logger.info(f"[DONE] Вторичная обработка завершена. Чанков: {audio_chunks_count}\n" + "="*50)

    except Exception as e:
        logger.error(f"[ERROR] {e}", exc_info=True)
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

async def handle_device_registration(websocket, data):
    conn = None
    cursor = None
    try:
        mac = data.get("MAC")
        device_name = data.get("DeviceName")
        password = data.get("Password")
        print(data)
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        
        websocket_id = id(websocket)
        
        cursor.execute("SELECT mac FROM devices WHERE device_name = %s AND mac != %s", (device_name, mac))
        if cursor.fetchone():
            await async_send(websocket, {"status": "error", "message": "Это имя устройства уже занято. Пожалуйста, выберите другое."})
            return
        
        cursor.execute("SELECT * FROM devices WHERE mac = %s", (mac,))
        device = cursor.fetchone()
        
        response = {"status": "success", "message": "Данные успешно обработаны!"}
        
        if device:
            updates = []
            params = []
            if device['device_name'] != device_name:
                cursor.execute("SELECT id FROM devices WHERE device_name = %s AND mac != %s", (device_name, mac))
                if cursor.fetchone():
                    await async_send(websocket, {"status": "error", "message": "Это имя устройства уже занято. Пожалуйста, выберите другое."})
                    return
            
            if device['device_name'] != device_name:
                updates.append("device_name = %s")
                params.append(device_name)
            
            if device['password'] != password:
                updates.append("password = %s")
                updates.append("access_list = ''")
                params.append(password)

            updates.append("websocket_id = %s")
            params.append(websocket_id)
            
            query = f"UPDATE devices SET {', '.join(updates)} WHERE mac = %s"
            params.append(mac)
            cursor.execute(query, params)
            
            cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
            updated_device = cursor.fetchone()
            device_id = updated_device['id'] if updated_device else None
            
            if device.get('user_id'):
                cursor.execute("SELECT login FROM users WHERE id = %s", (device['user_id'],))
                user = cursor.fetchone()
                if user:
                    response["user_login"] = user['login']
        else:
            cursor.execute("SELECT id FROM devices WHERE device_name = %s", (device_name,))
            if cursor.fetchone():
                await async_send(websocket, {"status": "error", "message": "Это имя устройства уже занято"})
                return
            
            cursor.execute(
                "INSERT INTO devices (mac, device_name, password, access_list, websocket_id, user_id) "
                "VALUES (%s, %s, %s, '', %s, NULL)",
                (mac, device_name, password, websocket_id)
            )
            device_id = cursor.lastrowid
        
        if device_id:
            cursor.execute("""
                SELECT 
                    m.id,
                    CASE
                        WHEN m.send_type = 'Вы' THEN 'Вы'
                        WHEN m.send_type = 'Бот' THEN 'Бот'
                        ELSE d.device_name
                    END AS sender,
                    m.text,
                    m.time
                FROM messages m
                LEFT JOIN devices d 
                    ON m.send_type = CAST(d.id AS CHAR) 
                    AND m.send_type != 'Вы' 
                    AND m.send_type != 'Бот'
                WHERE m.recipient_device_id = %s
                ORDER BY m.time ASC
            """, (device_id,))
            messages = cursor.fetchall()
            
            history = []
            for msg in messages:
                history.append({
                    "id": msg['id'],
                    "sender": msg['sender'],
                    "text": msg['text'],
                    "time": msg['time'].strftime('%Y-%m-%d %H:%M:%S')
                })
            
            response["history"] = history
        
        conn.commit()
        await async_send(websocket, response)
        print(response)
        
    except Exception as e:
        logger.error(f"Ошибка регистрации устройства: {e}")
        await async_send(websocket, {
            "status": "error", 
            "message": f"Произошла ошибка при обработке данных: {str(e)}"
        })
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

async def websocket_handler(websocket):
    client_id = id(websocket)
    active_connections[websocket] = client_id
    id_to_websocket[client_id] = websocket
    last_ping_times[websocket] = time.time()
    
    logger.info(f"New connection: {client_id}")
    
    try:
        async for message in websocket:
            try:
                decoded = base64.b64decode(message).decode('utf-8').strip().replace('\0x00', '')
                data = json.loads(decoded)
                
                # Обновляем пинг
                last_ping_times[websocket] = time.time()
                
                if data.get("type") == "ping":
                    continue
                
                if "DeviceName" in data:
                    await handle_device_registration(websocket, data)
                elif "command" in data:
                    await handle_command(websocket, data)
                elif "command_to_device" in data:
                    await handle_target_command(websocket, data)
                elif data.get("type") == "web_client_auth":
                    await handle_web_client_auth(websocket, data)

            except json.JSONDecodeError as e:
                logger.error(f"JSON Error: {e}")
                await async_send(websocket, {"status": "error", "message": f"Invalid JSON: {str(e)}"})
            except Exception as e:
                logger.error(f"Handler Error: {e}")
                await async_send(websocket, {"status": "error", "message": "Internal server error"})

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logger.error(f"Critical WS Error: {e}")
    finally:
        logger.info(f"Disconnected: {client_id}")
        if websocket in active_connections: del active_connections[websocket]
        if client_id in id_to_websocket: del id_to_websocket[client_id]
        if websocket in last_ping_times: del last_ping_times[websocket]
            
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("UPDATE devices SET websocket_id = NULL WHERE websocket_id = %s", (client_id,))
            conn.commit()
            conn.close()
        except: pass

async def check_pings():
    while True:
        try:
            now = time.time()
            to_remove = []
            for ws, last_time in list(last_ping_times.items()):
                if now - last_time > PING_TIMEOUT:
                    to_remove.append(ws)
            for ws in to_remove:
                last_ping_times.pop(ws, None)
                active_connections.pop(ws, None)
                try:
                    await ws.close()
                except:
                    pass
        except Exception as e:
            pass
        await asyncio.sleep(10)

async def main():
    asyncio.create_task(check_pings())
    async with websockets.serve(
        websocket_handler, 
        "0.0.0.0", 
        8114,
        ping_interval=None, 
        max_size=10 * 1024 * 1024
    ):
        logger.info("WebSocket Server started on 8114")
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())