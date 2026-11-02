import hashlib
import jwt
import bcrypt
import logging
import aiomysql
from app.config.settings import JWT_SECRET
from app.database.connection import get_async_db_connection
from app.websocket_server.state import mac_to_websocket, ws_to_mac
from app.websocket_server.utils import async_send

logger = logging.getLogger("WS_Server")

async def handle_account_sync(websocket, data):
    conn = None; cursor = None
    try:
        token = data.get('token')
        mac = data.get('mac')
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            user_id = payload['user_id']
        except:
            await async_send(websocket, {"status": "error", "message": "Недействительный токен"})
            return

        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        await cursor.execute("UPDATE devices SET user_id = %s WHERE mac = %s", (user_id, mac))
        
        await cursor.execute("SELECT id FROM dialogs WHERE user_id = %s ORDER BY created_at DESC LIMIT 1", (user_id,))
        dialog = await cursor.fetchone()
        
        history = []
        if dialog:
            await cursor.execute("""
                SELECT m.id, CASE WHEN m.send_type = 'Вы' THEN 'Вы' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender, m.text, m.created_at as time
                FROM messages m 
                LEFT JOIN devices d ON m.send_type COLLATE utf8mb4_general_ci = CAST(d.id AS CHAR) COLLATE utf8mb4_general_ci AND m.send_type NOT IN ('Вы', 'Бот')
                WHERE m.dialog_id = %s ORDER BY m.created_at ASC
            """, (dialog['id'],))
            for msg in await cursor.fetchall():
                msg_time = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else msg['time']
                history.append({"id": msg['id'], "sender": msg['sender'], "text": msg['text'], "time": msg_time})
        
        await cursor.execute("SELECT login FROM users WHERE id = %s", (user_id,))
        user_rec = await cursor.fetchone()

        await conn.commit()
        await async_send(websocket, {
            "type": "account_sync_success",
            "history": history,
            "user_login": user_rec['login'] if user_rec else "User",
            "status": "success"
        })
        logger.info(f"[AUTH] Аккаунт пользователя {user_rec['login'] if user_rec else user_id} привязан к устройству {mac}")
    except Exception as e:
        logger.error(f"[SYNC ERROR] Ошибка синхронизации: {e}", exc_info=True)
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()

async def handle_web_client_auth(websocket, data):
    conn = None; cursor = None
    try:
        token = data.get('token'); login = data.get('login')
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            user_id = payload['user_id']
        except:
            await async_send(websocket, {"status": "error", "message": "Invalid token"})
            await websocket.close(); return
        
        mac = f"WEB{hashlib.md5(str(token).encode()).hexdigest()[:13]}"
        device_name = f"Браузер {login} {mac}"
        
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        await cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
        device = await cursor.fetchone()
        
        if device: 
            await cursor.execute("UPDATE devices SET device_name = %s, is_online = TRUE, user_id = %s WHERE mac = %s", (device_name, user_id, mac))
        else: 
            await cursor.execute("INSERT INTO devices (mac, device_name, password, is_online, user_id) VALUES (%s, %s, '123', TRUE, %s)", (mac, device_name, user_id))
        
        mac_to_websocket[mac] = websocket; ws_to_mac[websocket] = mac
        await conn.commit()
        
        await async_send(websocket, {"status": "success", "message": "Данные успешно обработаны!", "history": []})
    except Exception as e:
        logger.error(f"[AUTH ERROR] Ошибка WEB авторизации: {e}", exc_info=True)
        await async_send(websocket, {"status": "error", "message": str(e)})
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()

async def handle_device_registration(websocket, data):
    conn = None; cursor = None
    try:
        mac = data.get("MAC")
        device_name = data.get("DeviceName")
        password = data.get("Password")

        logger.info(f"[AUTH] Запрос регистрации: MAC={mac}, Name={device_name}")

        if not mac or not device_name or not password:
            await async_send(websocket, {"status": "error", "message": "Не все поля заполнены"})
            return

        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        # Хэшируем пароль
        hashed_pw = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        # 1. Проверяем, есть ли уже этот MAC
        await cursor.execute("SELECT * FROM devices WHERE mac = %s", (mac,))
        device_by_mac = await cursor.fetchone()

        # 2. Проверяем, есть ли уже такое ИМЯ устройства
        await cursor.execute("SELECT * FROM devices WHERE device_name = %s", (device_name,))
        device_by_name = await cursor.fetchone()

        response = {"status": "success", "message": "Данные успешно обработаны!"}

        if device_by_mac:
            # Сценарий А: Тот же MAC — просто обновляем имя и пароль
            logger.info(f"[AUTH] Обновляем устройство по MAC={mac} -> имя '{device_name}'")
            await cursor.execute(
                "UPDATE devices SET device_name = %s, password = %s, is_online = TRUE WHERE id = %s",
                (device_name, hashed_pw, device_by_mac['id'])
            )
            device_id = device_by_mac['id']
            if device_by_mac.get('user_id'):
                await cursor.execute("SELECT login FROM users WHERE id = %s", (device_by_mac['user_id'],))
                u = await cursor.fetchone()
                if u: response['user_login'] = u['login']

        elif device_by_name:
            # Сценарий Б: Имя совпало, но MAC новый (переустановка на телефоне / смена сетевой карты)
            # Проверяем пароль от этого имени устройства
            if not bcrypt.checkpw(password.encode('utf-8'), device_by_name['password'].encode('utf-8')):
                logger.warning(f"[AUTH] Попытка перепривязать '{device_name}' с неверным паролем!")
                await async_send(websocket, {"status": "error", "message": "Имя занято. Введен неверный пароль."})
                return

            logger.info(f"[AUTH] Перепривязываем '{device_name}' на НОВЫЙ MAC={mac}")
            await cursor.execute(
                "UPDATE devices SET mac = %s, is_online = TRUE WHERE id = %s",
                (mac, device_by_name['id'])
            )
            device_id = device_by_name['id']
            if device_by_name.get('user_id'):
                await cursor.execute("SELECT login FROM users WHERE id = %s", (device_by_name['user_id'],))
                u = await cursor.fetchone()
                if u: response['user_login'] = u['login']

        else:
            # Сценарий В: Абсолютно новое устройство
            logger.info(f"[AUTH] Создаем новое устройство: MAC={mac}, Name={device_name}")
            await cursor.execute(
                "INSERT INTO devices (mac, device_name, password, is_online) VALUES (%s, %s, %s, TRUE)",
                (mac, device_name, hashed_pw)
            )
            device_id = cursor.lastrowid
            
        mac_to_websocket[mac] = websocket
        ws_to_mac[websocket] = mac
        
        # Подгружаем историю диалогов
        history = []
        user_id = device_by_mac.get('user_id') if device_by_mac else (device_by_name.get('user_id') if device_by_name else None)
        
        if user_id:
            await cursor.execute("SELECT id FROM dialogs WHERE user_id = %s ORDER BY created_at DESC LIMIT 1", (user_id,))
            dialog = await cursor.fetchone()
            if dialog:
                await cursor.execute("""
                    SELECT m.id, CASE WHEN m.send_type = 'Вы' THEN 'Вы' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender, m.text, m.created_at as time
                    FROM messages m LEFT JOIN devices d ON m.send_type COLLATE utf8mb4_general_ci = CAST(d.id AS CHAR) COLLATE utf8mb4_general_ci AND m.send_type NOT IN ('Вы', 'Бот')
                    WHERE m.dialog_id = %s ORDER BY m.created_at ASC
                """, (dialog['id'],))
                for msg in await cursor.fetchall():
                    msg_time = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else msg['time']
                    history.append({"id": msg['id'], "sender": msg['sender'], "text": msg['text'], "time": msg_time})
        else:
            await cursor.execute("""
                SELECT m.id, CASE WHEN m.send_type = 'Вы' THEN 'Вы' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender, m.text, m.created_at as time
                FROM messages m LEFT JOIN devices d ON m.send_type COLLATE utf8mb4_general_ci = CAST(d.id AS CHAR) COLLATE utf8mb4_general_ci AND m.send_type NOT IN ('Вы', 'Бот')
                WHERE m.recipient_device_id = %s ORDER BY m.created_at ASC
            """, (device_id,))
            for msg in await cursor.fetchall():
                msg_time = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else msg['time']
                history.append({"id": msg['id'], "sender": msg['sender'], "text": msg['text'], "time": msg_time})
        
        response["history"] = history
        await conn.commit()
        await async_send(websocket, response)
        logger.info(f"[AUTH] ✅ Устройство {mac} ('{device_name}') успешно авторизовано.")

    except Exception as e:
        logger.error(f"[AUTH ERROR] {e}", exc_info=True)
        await async_send(websocket, {"status": "error", "message": str(e)})
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()