import base64
import json
import logging
import asyncio
import aiomysql
from datetime import datetime
from app.database.connection import get_async_db_connection
from app.services.ai_service import ai_instance
from app.websocket_server import state
from app.websocket_server.utils import async_send, broadcast_to_user, get_device_type
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

# =========================================================================
# СПИСОК И ОПИСАНИЕ ВСЕХ ВОЗМОЖНОСТЕЙ СИСТЕМЫ
# =========================================================================
ACTION_DESCRIPTIONS = {
    "открытие ссылки": "строго полный URL адрес (например https://youtube.com)",
    "открытие файла": "строго точное имя программы или путь (например 'steam', 'telegram', 'calc', 'pubg')",
    "завершение процесса": "имя процесса (без .exe)",
    "музыка": "строго одно из: 'включить', 'выключить', 'следующий', 'предыдущий'",
    "смена голоса": "строго одно из: Aoede, Puck, Kore, Charon",
    "изменение громкости": "целое число от 0 до 100 (например 20, 50, 80)",
    "изменение яркости": "целое число от 0 до 100",
    "выключить микрофон": "любой текст (доступно только на веб-сайте)",
    "название диалога": "краткое название для текущего диалога (1-3 слова)",
    "движение": "строго одно из: 'вперед', 'назад', 'влево', 'вправо'"
}

CLIENT_ACTIONS = {
    "web": [
        "название диалога",
        "выключить микрофон",
        "смена голоса"
    ],
    "robot": [
        "название диалога",
        "движение",
        "смена голоса",
        "музыка",
        "изменение громкости"
    ],
    "phone": [
        "открытие ссылки",
        "открытие файла",
        "завершение процесса",
        "музыка",
        "смена голоса",
        "изменение громкости",
        "изменение яркости",
        "название диалога"
    ],
    "pc": [
        "открытие ссылки",
        "открытие файла",
        "завершение процесса",
        "музыка",
        "смена голоса",
        "изменение громкости",
        "изменение яркости",
        "название диалога"
    ]
}

CLIENT_NAMES = {
    "web": "Веб-сайт (Браузер)",
    "robot": "Робот (Raspberry Pi)",
    "phone": "Телефон (Android)",
    "pc": "Компьютер (ПК Windows)"
}

ALL_ACTIONS = list(ACTION_DESCRIPTIONS.keys())

def format_action_list(action_keys):
    caps = []
    for k in action_keys:
        desc = ACTION_DESCRIPTIONS.get(k, "параметр действия")
        caps.append(f"  • {k} (параметр: {desc})")
    return "\n".join(caps)

def normalize_client_type(raw_type):
    t = str(raw_type or '').strip().lower()
    if t in CLIENT_ACTIONS:
        return t
    if any(k in t for k in ['phone', 'тел', 'android']): return 'phone'
    if any(k in t for k in ['robot', 'робот', 'pi']): return 'robot'
    if any(k in t for k in ['web', 'браузер', 'сайт']): return 'web'
    return 'pc'

def get_user_online_devices(user_id, current_ws, sender_name, device_type_code):
    current_type = normalize_client_type(device_type_code)
    current_dev = {
        "ws": current_ws,
        "name": sender_name,
        "type": current_type,
        "type_ru": get_device_type(type_str=current_type),
        "is_current": True
    }
    devices = [current_dev]

    if user_id:
        user_ws_set = state.user_connections.get(user_id, set())
        for ws in user_ws_set:
            if ws == current_ws:
                continue
            session = state.ws_sessions.get(ws)
            if not session:
                continue
            d_name = session.get('device_name') or 'Устройство'
            d_type = normalize_client_type(session.get('device_type') or 'pc')
            devices.append({
                "ws": ws,
                "name": d_name,
                "type": d_type,
                "type_ru": get_device_type(type_str=d_type),
                "is_current": False
            })

    return devices

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

def get_target_websocket(user_id, target_device_name, fallback_ws, action_type=None):
    """Поиск целевого веб-сокета в RAM по имени или типу устройства с умной маршрутизацией."""
    clean_name = target_device_name.lower().strip() if target_device_name else ""
    user_ws_set = state.user_connections.get(user_id, set()) if user_id else set()
    
    # 1. Прямое совпадение по имени или типу
    if clean_name:
        for ws in user_ws_set:
            session = state.ws_sessions.get(ws)
            if not session: continue
            d_name = session.get('device_name', '').lower()
            d_type = normalize_client_type(session.get('device_type', 'pc'))
            
            if clean_name in d_name or d_name in clean_name:
                return ws
            if any(k in clean_name for k in ["комп", "пк", "pc", "компе"]) and d_type == "pc":
                return ws
            if any(k in clean_name for k in ["телефон", "phone", "смартфон", "мобил"]) and d_type == "phone":
                return ws
            if any(k in clean_name for k in ["робот", "robot", "бот", "пибот"]) and d_type == "robot":
                return ws
            if any(k in clean_name for k in ["браузер", "web", "сайт"]) and d_type == "web":
                return ws

    # 2. Если fallback_ws поддерживает действие — остаемся на нем
    if action_type:
        fallback_session = state.ws_sessions.get(fallback_ws, {})
        fallback_type = normalize_client_type(fallback_session.get('device_type', 'pc'))
        if action_type in CLIENT_ACTIONS.get(fallback_type, []):
            return fallback_ws

        # 3. Умная переадресация: fallback не умеет это делать, перенаправляем на подходящее онлайн-устройство пользователя
        for ws in user_ws_set:
            session = state.ws_sessions.get(ws, {})
            d_type = normalize_client_type(session.get('device_type', 'pc'))
            if action_type in CLIENT_ACTIONS.get(d_type, []):
                logger.info(f"🔄 [ROUTING] Действие '{action_type}' умно перенаправлено с '{fallback_type}' на '{session.get('device_name')}' ({d_type})")
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
            norm_type = normalize_client_type(device_type_code)
            if 'dialog_id' in data and client_dialog_id is None: 
                create_new = True
            elif norm_type == 'robot' and not client_dialog_id and not session.get('robot_dialog_id'):
                create_new = True
            elif client_dialog_id:
                await cursor.execute("SELECT id FROM dialogs WHERE id = %s AND user_id = %s", (client_dialog_id, user_id))
                if not await cursor.fetchone(): create_new = True
                else: dialog_id = client_dialog_id
            elif norm_type == 'robot' and session.get('robot_dialog_id'):
                # Важно: проверяем, что робот не ссылается на удаленный диалог в БД!
                await cursor.execute("SELECT id FROM dialogs WHERE id = %s AND user_id = %s", (session['robot_dialog_id'], user_id))
                if await cursor.fetchone():
                    dialog_id = session['robot_dialog_id']
                else:
                    session.pop('robot_dialog_id', None)
                    create_new = True
            else:
                await cursor.execute("SELECT id FROM dialogs WHERE user_id = %s ORDER BY created_at DESC LIMIT 1", (user_id,))
                dlg = await cursor.fetchone()
                if dlg: dialog_id = dlg['id']
                else: create_new = True
                    
            if create_new:
                dialog_name = f"Робот ({sender_name})" if norm_type == 'robot' else "Новый диалог"
                await cursor.execute("INSERT INTO dialogs (name, user_id) VALUES (%s, %s)", (dialog_name, user_id))
                dialog_id = cursor.lastrowid
                await conn.commit()
                if norm_type == 'robot':
                    session['robot_dialog_id'] = dialog_id
                # Рассылаем создание нового диалога ВСЕМ активным устройствам пользователя
                await broadcast_to_user(user_id, {"type": "dialog_created", "dialog_id": dialog_id, "name": dialog_name})

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

                # Сразу транслируем сообщение пользователя на все другие устройства,
                # чтобы на них пузырь пользователя гарантированно создавался ПЕРВЫМ (до ответа бота).
                if user_id:
                    await broadcast_to_user(user_id, {
                        "type": "new_message",
                        "dialog_id": dialog_id,
                        "message_id": user_msg_id,
                        "ui_msg_id": ui_msg_id,
                        "sender": "Вы",
                        "device_name": sender_name,
                        "text": db_user_text,
                        "actions": []
                    }, exclude_ws=websocket)

                # Строгая хронологическая сортировка по id ASC (гарантирует порядок)
                await cursor.execute("""
                    SELECT sender, text FROM messages
                    WHERE dialog_id = %s AND id < %s AND sender != 'Действие' ORDER BY id ASC
                """, (dialog_id, user_msg_id))
                
                raw_history = await cursor.fetchall()
                if raw_history:
                    for msg in raw_history[-HISTORY_LIMIT:]:
                        if msg.get('sender') == 'Действие' or '⚡' in msg.get('text', ''):
                            continue
                        role = "user" if msg['sender'] == 'Вы' else "model"
                        formatted_history.append({"role": role, "parts": [{"text": msg['text']}]})

                is_first_message = (len(raw_history) == 0)
                is_unnamed_dialog = False
                await cursor.execute("SELECT name FROM dialogs WHERE id = %s", (dialog_id,))
                cur_dlg = await cursor.fetchone()
                if cur_dlg:
                    d_name_val = cur_dlg.get('name') or ''
                    if d_name_val in ['Новый диалог', 'Новый чат', ''] or d_name_val.startswith('Робот ('):
                        is_unnamed_dialog = True
        else:
            user_msg_id = int(datetime.now().timestamp() * 1000)
            if message_history:
                for m in message_history[-HISTORY_LIMIT:]:
                    role = "user" if m.get('role') == 'user' else "model"
                    formatted_history.append({"role": role, "parts": [{"text": m.get('content', '')}]})
            is_first_message = len(formatted_history) == 0
            is_unnamed_dialog = True

        needs_dialog_naming = is_first_message or is_unnamed_dialog

        if sender_ws and ui_msg_id and dialog_id:
            await async_send(sender_ws, {"type": "msg_id_map", "ui_msg_id": ui_msg_id, "user_msg_id": user_msg_id, "bot_msg_id": bot_message_id})

        # === ОПРЕДЕЛЯЕМ ОНЛАЙН-УСТРОЙСТВА И ИХ ВОЗМОЖНОСТИ ===
        all_online_devs = get_user_online_devices(user_id, websocket, sender_name, device_type_code)
        online_types = set([d["type"] for d in all_online_devs])

        # Собираем действия, доступные хотя бы на одном из устройств онлайн:
        allowed_action_keys = []
        for c_type in ["pc", "phone", "robot", "web"]:
            if c_type in online_types:
                for act in CLIENT_ACTIONS[c_type]:
                    if act not in allowed_action_keys:
                        allowed_action_keys.append(act)

        if not allowed_action_keys:
            allowed_action_keys = CLIENT_ACTIONS.get(normalize_client_type(device_type_code), ["название диалога", "смена голоса"])

        allowed_actions = {act: ACTION_DESCRIPTIONS.get(act, "параметр действия") for act in allowed_action_keys}

        # Список подключенных устройств пользователя
        devices_lines = []
        for d in all_online_devs:
            mark = " (текущее устройство собеседника)" if d["is_current"] else ""
            devices_lines.append(f"• «{d['name']}» — {d['type_ru']}{mark}")
        current_dev_desc = "\n".join(devices_lines)

        # Возможности ТОЛЬКО тех платформ, которые сейчас онлайн в сети пользователя
        caps_blocks = []
        for c_type in ["pc", "phone", "robot", "web"]:
            if c_type in online_types:
                caps_blocks.append(f"• {CLIENT_NAMES[c_type]}:\n{format_action_list(CLIENT_ACTIONS[c_type])}")
        platforms_capabilities = "\n\n".join(caps_blocks)

        system_instruction = f"""Ты — ИИ-помощник {name}. Твой собеседник говорит через устройство: «{sender_name}» ({get_device_type(type_str=normalize_client_type(device_type_code))}).
Тебе в реальном времени передается видеопоток с камеры пользователя.

УСТРОЙСТВА В СЕТИ:
{current_dev_desc}

ВОЗМОЖНОСТИ УСТРОЙСТВ:
{platforms_capabilities}

УПРАВЛЕНИЕ УСТРОЙСТВАМИ (ИНСТРУМЕНТ send_device_commands):
1. Для выполнения действий вызывай инструмент `send_device_commands`. В поле `target_device` передавай точное имя целевого устройства.
2. Каждое действие поддерживается только соответствующим типом устройства из списка выше. Если текущее устройство собеседника не поддерживает действие, но в сети есть подходящее онлайн-устройство пользователя — направь команду на него.
3. «открытие файла» — запуск любых программ, приложений и игр (например: 'steam', 'calc', 'telegram').
4. «открытие ссылки» — полный URL-адрес (например: https://youtube.com).
5. «музыка» — допустимые параметры: 'включить', 'выключить', 'следующий', 'предыдущий'.
6. «название диалога» — если диалог только начался или не имеет названия, дай ему краткое емкое название (1-3 слова) через это действие.
7. Твой текстовый ответ озвучивается пользователю голосом через динамики, поэтому отвечай чистым разговорным текстом без кода, Markdown-таблиц и технических тегов."""

        prompt_text_to_send = command if command else None

        logger.info(f"[API] Отправляю в Фасад (Gemini Live 3.1)... Онлайн устройств: {len(all_online_devs)} | Доступных действий: {len(allowed_action_keys)} | Первое сообщение: {needs_dialog_naming}")

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
                    if "⚡" in text_chunk or "[Действие" in text_chunk or "action_type" in text_chunk: continue
                    if any(t in text_chunk.lower() for t in ["</td>", "<script", "<html>", "</div>", "<table>", "<svg", "<tr>"]): continue
                    final_bot_text_full += text_chunk + " "
                    logger.info(f"[TTS] Фасад: {text_chunk.strip()}")
                    # Отправляем на устройство инициатора
                    if sender_ws:
                        await async_send(sender_ws, {
                            "type": "new_message",
                            "dialog_id": dialog_id,
                            "message_id": bot_message_id,
                            "ui_msg_id": ui_msg_id,
                            "sender": "Бот",
                            "text": text_chunk,
                            "actions": []
                        })
                    # Синхронизируем текст бота в реальном времени на все остальные устройства пользователя
                    if user_id:
                        await broadcast_to_user(user_id, {
                            "type": "new_message",
                            "dialog_id": dialog_id,
                            "message_id": bot_message_id,
                            "ui_msg_id": ui_msg_id,
                            "sender": "Бот",
                            "text": text_chunk,
                            "actions": []
                        }, exclude_ws=websocket)

                elif chunk["type"] == "commands":
                    if chunk["commands"]: has_commands = True
                    extracted_commands = chunk["commands"]
                    
                    for cmd in extracted_commands:
                        target_device_name = cmd.get('target_device', '').strip()
                        actions = cmd.get('actions', [])
                        if not actions: continue

                        acts_summary = ", ".join([f"{a.get('action_type')}: '{a.get('action_value')}'" for a in actions])
                        logger.info(f"🛠️ [TOOL CALL] ИИ вызвал: [{acts_summary}] -> цель: '{target_device_name or sender_name}'")

                        ws_actions_map = {}
                        for act in actions:
                            act_type = str(act.get('action_type', '')).strip().lower()
                            if act_type == "открытие приложения":
                                act['action_type'] = "открытие файла"
                                act_type = "открытие файла"

                            if act_type == "название диалога" and dialog_id:
                                new_name = str(act.get('action_value', '')).strip()
                                await cursor.execute("UPDATE dialogs SET name = %s WHERE id = %s", (new_name, dialog_id))
                                await conn.commit()
                                # Оповещаем все устройства пользователя о новом имени диалога
                                if user_id:
                                    await broadcast_to_user(user_id, {"type": "dialog_renamed", "dialog_id": dialog_id, "name": new_name})
                                elif sender_ws:
                                    await async_send(sender_ws, {"type": "dialog_renamed", "dialog_id": dialog_id, "name": new_name})
                                ws_actions_map.setdefault(websocket, []).append(act)
                            else:
                                tgt_ws = get_target_websocket(user_id, target_device_name, fallback_ws=websocket, action_type=act_type)
                                if tgt_ws:
                                    ws_actions_map.setdefault(tgt_ws, []).append(act)
                                else:
                                    logger.warning(f"⚠️ [DIRECT DISPATCH] Не найден сокет для действия '{act_type}' на '{target_device_name}'")

                        for tgt_ws, real_actions in ws_actions_map.items():
                            tgt_session = state.ws_sessions.get(tgt_ws, {})
                            tgt_name = tgt_session.get('device_name', sender_name)
                            logger.info(f"🚀 [DIRECT DISPATCH] Фасад отправляет {len(real_actions)} действий на '{tgt_name}'")

                            # Сохраняем и отправляем каждое действие отдельным сообщением с маркировкой
                            for act in real_actions:
                                a_type = act.get('action_type', '')
                                a_val = str(act.get('action_value', '')).strip()
                                if a_type == "название диалога":
                                    action_label = f"⚡ [Действие]: название диалога — «{a_val}»"
                                else:
                                    action_label = f"⚡ [Действие на «{tgt_name}»]: {a_type} — {a_val}" if a_val else f"⚡ [Действие на «{tgt_name}»]: {a_type}"

                                action_msg_id = None
                                if dialog_id:
                                    await cursor.execute(
                                        "INSERT INTO messages (sender, device_name, text, dialog_id) VALUES ('Действие', %s, %s, %s)",
                                        (tgt_name, action_label, dialog_id)
                                    )
                                    action_msg_id = cursor.lastrowid
                                    await conn.commit()
                                else:
                                    action_msg_id = int(time.time() * 1000)

                                # 1. Отправляем на целевое устройство для исполнения действия и отображения
                                await async_send(tgt_ws, {
                                    "type": "new_message",
                                    "dialog_id": dialog_id,
                                    "message_id": action_msg_id,
                                    "user_msg_id": user_msg_id if tgt_ws == websocket else None,
                                    "sender": "Действие",
                                    "device_name": tgt_name,
                                    "text": action_label,
                                    "actions": [act]
                                })

                                # 2. Информационно синхронизируем плашку действия на ВСЕ остальные устройства пользователя
                                if user_id:
                                    await broadcast_to_user(user_id, {
                                        "type": "new_message",
                                        "dialog_id": dialog_id,
                                        "message_id": action_msg_id,
                                        "user_msg_id": user_msg_id if sender_ws != tgt_ws else None,
                                        "sender": "Действие",
                                        "device_name": tgt_name,
                                        "text": action_label,
                                        "actions": []
                                    }, exclude_ws=tgt_ws)
                                elif tgt_ws != websocket and sender_ws:
                                    await async_send(sender_ws, {
                                        "type": "new_message",
                                        "dialog_id": dialog_id,
                                        "message_id": action_msg_id,
                                        "user_msg_id": user_msg_id,
                                        "sender": "Действие",
                                        "device_name": tgt_name,
                                        "text": action_label,
                                        "actions": []
                                    })

                elif chunk["type"] == "audio":
                    audio_chunks_count += 1
                    # Голос воспроизводится строго на устройстве, с которого говорят
                    if sender_ws: await async_send(sender_ws, {"type": "audio_chunk", "audio_base64": base64.b64encode(chunk["data"]).decode('utf-8')})
        
        if dialog_id and user_msg_id and bot_message_id:
            if (audio_bytes or is_streaming):
                if final_user_text_full.strip():
                    # Сохраняем исходное created_at, предотвращая авто-сдвиг даты в MySQL при UPDATE
                    await cursor.execute("UPDATE messages SET text = %s, created_at = created_at WHERE id = %s", (final_user_text_full.strip(), user_msg_id))
                    # Обновляем текст на устройстве-инициаторе
                    if sender_ws:
                        await async_send(sender_ws, {
                            "type": "new_message",
                            "dialog_id": dialog_id,
                            "message_id": user_msg_id,
                            "ui_msg_id": ui_msg_id,
                            "sender": "Вы",
                            "device_name": sender_name,
                            "text": final_user_text_full.strip(),
                            "actions": []
                        })
                    # Синхронизируем распознанный голос на другие устройства (обновление заготовки)
                    if user_id:
                        await broadcast_to_user(user_id, {
                            "type": "new_message",
                            "dialog_id": dialog_id,
                            "message_id": user_msg_id,
                            "ui_msg_id": ui_msg_id,
                            "sender": "Вы",
                            "device_name": sender_name,
                            "text": final_user_text_full.strip(),
                            "actions": []
                        }, exclude_ws=websocket)

            if not final_bot_text_full.strip():
                # Удаляем пустую текстовую заготовку бота, чтобы не оставался пустой пузырь
                await cursor.execute("DELETE FROM messages WHERE id = %s", (bot_message_id,))
                if audio_chunks_count == 0 and not has_commands:
                    await cursor.execute("DELETE FROM messages WHERE id = %s", (user_msg_id,))
            else:
                # Сохраняем исходное created_at
                await cursor.execute("UPDATE messages SET text = %s, created_at = created_at WHERE id = %s", (final_bot_text_full.strip(), bot_message_id))
            await conn.commit()
            
        if not dialog_id:
            if not final_bot_text_full.strip() and audio_chunks_count == 0 and not has_commands:
                if sender_ws: await async_send(sender_ws, {"type": "delete_message", "ui_msg_id": ui_msg_id})
            else:
                if sender_ws: await async_send(sender_ws, {"type": "new_message", "message_id": None, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": "", "actions": []})
        elif sender_ws:
            final_bot_id = bot_message_id if final_bot_text_full.strip() else None
            finish_msg = {
                "type": "new_message",
                "dialog_id": dialog_id,
                "message_id": final_bot_id,
                "ui_msg_id": ui_msg_id,
                "sender": "Бот",
                "text": "",
                "actions": []
            }
            await async_send(sender_ws, finish_msg)
            if user_id:
                await broadcast_to_user(user_id, finish_msg, exclude_ws=websocket)

        logger.info(f"[DONE] Фасад отработал.\n" + "="*50)

    except Exception as e:
        logger.error(f"[ERROR] {e}", exc_info=True)
        try:
            if cursor and bot_message_id and user_msg_id:
                await cursor.execute("DELETE FROM messages WHERE id IN (%s, %s)", (bot_message_id, user_msg_id)); await conn.commit()
            sender_ws = websocket
            if sender_ws: await async_send(sender_ws, {"type": "delete_message", "ui_msg_id": ui_msg_id})
            if user_id: await broadcast_to_user(user_id, {"type": "delete_message", "ui_msg_id": ui_msg_id}, exclude_ws=websocket)
        except: pass
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()
        active_media_queues.pop(data.get('ui_msg_id', ''), None)
