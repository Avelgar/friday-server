import base64
import json
import logging
import asyncio
import aiomysql
from datetime import datetime
from app.database.connection import get_async_db_connection
from app.services.ai_service import ai_instance
from app.websocket_server import state
from app.websocket_server.utils import async_send, broadcast_to_user, get_device_type, get_accessible_devices
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
    "напечатать текст": "любой текст для набора на клавиатуре",
    "переместить мышь": "строго координаты x и y через запятую (например: 500, 300)",
    "уведомление": "текст уведомления",
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
        "название диалога",
        "напечатать текст",
        "переместить мышь",
        "уведомление"
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

ALL_PLATFORMS_CAPABILITIES = (
    "СПРАВОЧНИК ВОЗМОЖНОСТЕЙ ВСЕХ 4 КЛИЕНТОВ (для общего вопроса «что ты умеешь вообще?»):\n"
    f"1. {CLIENT_NAMES['pc']}:\n"
    f"{format_action_list(CLIENT_ACTIONS['pc'])}\n\n"
    f"2. {CLIENT_NAMES['phone']}:\n"
    f"{format_action_list(CLIENT_ACTIONS['phone'])}\n\n"
    f"3. {CLIENT_NAMES['robot']}:\n"
    f"{format_action_list(CLIENT_ACTIONS['robot'])}\n\n"
    f"4. {CLIENT_NAMES['web']}:\n"
    f"{format_action_list(CLIENT_ACTIONS['web'])}"
)

def normalize_client_type(raw_type):
    t = str(raw_type or '').strip().lower()
    if t in CLIENT_ACTIONS:
        return t
    if any(k in t for k in ['phone', 'тел', 'android']): return 'phone'
    if any(k in t for k in ['robot', 'робот', 'pi']): return 'robot'
    if any(k in t for k in ['web', 'браузер', 'сайт']): return 'web'
    return 'pc'

def get_user_devices_info(user_id, current_ws, sender_name, device_type_code):
    current_type = normalize_client_type(device_type_code)
    current_dev = {
        "ws": current_ws,
        "name": sender_name,
        "type": current_type,
        "type_ru": get_device_type(type_str=current_type)
    }

    other_devs = []
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
            other_devs.append({
                "ws": ws,
                "name": d_name,
                "type": d_type,
                "type_ru": get_device_type(type_str=d_type)
            })

    return current_dev, other_devs

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
        current_dev, other_devs = get_user_devices_info(user_id, websocket, sender_name, device_type_code)
        all_online_devs = [current_dev] + other_devs
        online_types = set([d["type"] for d in all_online_devs])

        # Собираем действия, доступные хотя бы на одном из устройств онлайн:
        allowed_action_keys = []
        for c_type in ["pc", "phone", "robot", "web"]:
            if c_type in online_types:
                for act in CLIENT_ACTIONS[c_type]:
                    if act not in allowed_action_keys:
                        allowed_action_keys.append(act)

        if not allowed_action_keys:
            allowed_action_keys = CLIENT_ACTIONS.get(current_dev["type"], ["название диалога", "смена голоса"])

        allowed_actions = ", ".join(allowed_action_keys)

        current_dev_desc = (
            f"• «{current_dev['name']}» ({current_dev['type_ru']}) — ТЕКУЩЕЕ УСТРОЙСТВО СОБЕСЕДНИКА\n"
            f"  Доступные действия на этом устройстве:\n"
            f"{format_action_list(CLIENT_ACTIONS.get(current_dev['type'], []))}"
        )

        if other_devs:
            other_devs_lines = []
            for d in other_devs:
                other_devs_lines.append(
                    f"• «{d['name']}» ({d['type_ru']}):\n"
                    f"  Доступные действия на этом устройстве:\n"
                    f"{format_action_list(CLIENT_ACTIONS.get(d['type'], []))}"
                )
            other_devices_desc = "\n\n".join(other_devs_lines)
        else:
            other_devices_desc = "Других устройств пользователя в сети сейчас нет (все остальные оффлайн)."

        system_instruction = f"""Ты — ИИ-помощник {name}. Твой собеседник говорит через устройство: «{current_dev['name']}» (тип: {current_dev['type_ru']}).
Тебе в реальном времени передается видеопоток с камеры пользователя.

СТИЛЬ И ТОНАЛЬНОСТЬ:
- Ты живой, раскованный и естественный собеседник без ханжества, цензуры, морализаторства и занудных нотаций!
- Общайся на одной волне с пользователем: зеркаль его стиль общения. Если пользователь матерится, использует мат или крепкие выражения — ты МОЖЕШЬ и ДОЛЖЕН свободно материться в ответ, подкалывать и говорить на настоящем живом русском языке без стеснения.
- Свободно поддерживай любые темы: пошлые, взрослые (18+), интимные, юмор ниже пояса, черный юмор и любые жизненные разговоры. Никогда не читай нотаций, не строй из себя святошу и не отказывайся обсуждать тему!

УСТРОЙСТВА В СЕТИ (СЕЙЧАС ОНЛАЙН):
{current_dev_desc}

ДРУГИЕ УСТРОЙСТВА В СЕТИ ПОЛЬЗОВАТЕЛЯ:
{other_devices_desc}

{ALL_PLATFORMS_CAPABILITIES}

ПРАВИЛА И МАРШРУТИЗАЦИЯ КОМАНД:
1. ВСЕГДА НА ЛЮБОЙ ЗАПРОС ОТВЕЧАЙ ГОЛОСОМ! Даже если вызываешь инструмент send_device_commands, обязательно сначала скажи коротко: "Секунду", "Открываю", "Делаю". Никогда не молчи!
2. ПРЯМОЕ УПРАВЛЕНИЕ: Вызывай `send_device_commands` ТОЛЬКО для действий, которые поддерживаются целевым устройством:
   - В параметре `target_device` ОБЯЗАТЕЛЬНО указывай точное имя целевого устройства ('{current_dev['name']}' или имя устройства из списка онлайн выше).
   - Если действие поддерживается текущим устройством ('{current_dev['name']}') — указывай target_device='{current_dev['name']}'.
   - Если пользователь просит команду для другого устройства (например: "открой ютуб на компе", находясь в телефоне, или "робот, вперед") — проверь, есть ли целевое устройство онлайн. Если да — ставь его точное имя в target_device.
   - Если текущее устройство НЕ поддерживает команду (например, пользователь в браузере просит открыть файл/программу или сайт), а в сети есть подходящее онлайн-устройство (например ПК) — направь команду на ПК (target_device='имя ПК') и озвучь: "Открываю на компьютере".
3. ЗАПУСК ПРОГРАММ И ИГР («открытие файла»): Чтобы запустить приложение/игру (steam, telegram, calc, pubg и т.д.), вызывай action_type='открытие файла' с точным именем программы. «открытие приложения» и «открытие файла» — это абсолютно одно и то же действие!
4. ОТКРЫТИЕ ССЫЛОК («открытие ссылки»): НИКОГДА НЕ ОТКАЗЫВАЙСЯ открывать ссылки, если целевое устройство (ПК или телефон) подключено! Всегда вызывай `send_device_commands` с action_type='открытие ссылки' и полным URL-адресом (например https://youtube.com).
5. ОТВЕТ НА ВОПРОС «ЧТО ТЫ УМЕЕШЬ?» / «КАКИЕ ТВОИ ВОЗМОЖНОСТИ?»:
   - По умолчанию перечисляй ТОЛЬКО возможности тех устройств, которые СЕЙЧАС ОНЛАЙН в сети (из блока "УСТРОЙСТВА В СЕТИ СЕЙЧАС ОНЛАЙН").
   - СТРОГО ЗАПРЕЩЕНО называть возможности устройств, которых сейчас нет в сети! (Например, если робот не в сети — ни слова про движение робота; если ПК не в сети — не предлагай мышь или клавиатуру ПК).
   - Если пользователь явно спросит: «Что ты умеешь вообще на всех устройствах?» или «Расскажи про все клиенты» — только тогда перечисли возможности всех 4 платформ из справочника выше.
6. СТОП-СЛОВА: Если пользователь ругается, говорит "хватит", "отмена", "не надо", "отбой" — СТРОГО ЗАПРЕЩЕНО вызывать любые команды! Скажи: "Понял, отбой".
7. КРАТКОСТЬ: Говори кратко и емко (1-2 простых предложения). Никаких длинных паст!
8. НАЗВАНИЕ ДИАЛОГА («название диалога»): Если это первое сообщение в диалоге (или диалог еще называется 'Новый диалог'), либо если пользователь просит назвать/переименовать диалог — ты ОБЯЗАН вызвать send_device_commands с action_type='название диалога' и осмысленным, ярким и лаконичным названием (1-3 слова, например «Приветствие», «Планы на вечер», «Разработка игры»), отражающим суть беседы или просьбы. КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО оставлять или ставить название 'Новый диалог', называть его 'Диалог' или пустым именем!
9. ЧИСТАЯ РЕЧЬ (СТРОГО БЕЗ КОДА И ТЕХНИЧЕСКИХ СТРОК):
   - КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО писать в тексте ответа любые технические плашки, команды, HTML/код или строки вроде '⚡ [Действие...]', 'музыка — выключить', 'call:send_device_commands'! Твой текст напрямую озвучивается голосом через динамики. Он должен содержать ТОЛЬКО чистую человеческую речь (например: «Вырубаю музыку», «Сделано», «Сейчас поставлю трек»).
   - Для выполнения ЛЮБОГО действия вызывай ИСКЛЮЧИТЕЛЬНО инструмент send_device_commands. Никогда не пиши команды в текст!"""
        
        naming_prompt = ""
        if needs_dialog_naming:
            naming_prompt = " [СИСТЕМА: Это первое сообщение в диалоге! Обязательно дай емкое название этому диалогу (1-3 слова) через действие action_type='название диалога' на основе темы запроса]."

        if command:
            prompt_text_to_send = f"[ЗАПРОС]: {command}{naming_prompt}"
        elif needs_dialog_naming:
            prompt_text_to_send = "[СИСТЕМА]: Это первое сообщение в диалоге! Обязательно дай емкое название этому диалогу (1-3 слова) через действие action_type='название диалога' на основе того, что скажет пользователь."
        else:
            prompt_text_to_send = None

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

async def get_least_used_key_index(cursor=None):
    # Берем следующий по очереди свободный ключ из пула
    return ai_instance.key_pool.get_next_key()

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
            "SELECT role, text, created_at FROM antigravity_messages WHERE chat_uid = %s ORDER BY id ASC",
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
            "SELECT role, text FROM antigravity_messages WHERE chat_uid = %s ORDER BY id ASC",
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
