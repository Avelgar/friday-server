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
            await cursor.execute("UPDATE devices SET device_name = %s, is_online = TRUE WHERE mac = %s", (device_name, mac))
        else: 
            await cursor.execute("INSERT INTO devices (mac, device_name, password, is_online, user_id) VALUES (%s, %s, '123', TRUE, %s)", (mac, device_name, user_id))
        
        mac_to_websocket[mac] = websocket; ws_to_mac[websocket] = mac
        await cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
        device_record = await cursor.fetchone()
        device_id = device_record['id']
        
        history =[]
        # ФИКС КОДИРОВКИ: COLLATE utf8mb4_general_ci
        await cursor.execute("""
            SELECT m.id, CASE WHEN m.send_type = 'Вы' THEN 'Вы' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender, m.text, m.created_at as time
            FROM messages m LEFT JOIN devices d ON m.send_type COLLATE utf8mb4_general_ci = CAST(d.id AS CHAR) COLLATE utf8mb4_general_ci AND m.send_type NOT IN ('Вы', 'Бот')
            WHERE m.recipient_device_id = %s ORDER BY m.created_at ASC
        """, (device_id,))
        for msg in await cursor.fetchall():
            msg_time = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else msg['time']
            history.append({"id": msg['id'], "sender": msg['sender'], "text": msg['text'], "time": msg_time})
        
        await conn.commit()
        await async_send(websocket, {"status": "success", "message": "Данные успешно обработаны!", "history": history})
    except Exception as e:
        logger.error(f"[AUTH ERROR] Ошибка WEB авторизации: {e}", exc_info=True)
        await async_send(websocket, {"status": "error", "message": str(e)})
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()

async def handle_device_registration(websocket, data):
    conn = None; cursor = None
    try:
        mac = data.get("MAC"); device_name = data.get("DeviceName"); password = data.get("Password")
        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        await cursor.execute("SELECT * FROM devices WHERE mac = %s", (mac,))
        device = await cursor.fetchone()
        response = {"status": "success", "message": "Данные успешно обработаны!"}
        
        if device:
            if not bcrypt.checkpw(password.encode('utf-8'), device['password'].encode('utf-8')):
                await async_send(websocket, {"status": "error", "message": "Неверный пароль устройства. В доступе отказано."})
                return
            await cursor.execute("UPDATE devices SET device_name = %s, is_online = TRUE WHERE id = %s", (device_name, device['id']))
            device_id = device['id']
            if device.get('user_id'):
                await cursor.execute("SELECT login FROM users WHERE id = %s", (device['user_id'],))
                user_rec = await cursor.fetchone()
                if user_rec: response['user_login'] = user_rec['login']
        else:
            await cursor.execute("SELECT id FROM devices WHERE device_name = %s", (device_name,))
            if await cursor.fetchone():
                await async_send(websocket, {"status": "error", "message": "Это имя устройства уже занято. Пожалуйста, выберите другое."})
                return
            hashed_pw = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            await cursor.execute("INSERT INTO devices (mac, device_name, password, is_online) VALUES (%s, %s, %s, TRUE)", (mac, device_name, hashed_pw))
            device_id = cursor.lastrowid
            
        mac_to_websocket[mac] = websocket; ws_to_mac[websocket] = mac
        
        history = []
        # ФИКС КОДИРОВКИ: COLLATE utf8mb4_general_ci
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
    except Exception as e:
        logger.error(f"[AUTH ERROR] Ошибка регистрации устройства: {e}", exc_info=True)
        await async_send(websocket, {"status": "error", "message": str(e)})
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()

async def handle_desktop_auth(websocket, data):
    conn = None; cursor = None
    try:
        token = data.get('token'); mac = data.get('mac'); device_name = data.get('device_name')
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            user_id = payload['user_id']
        except:
            await async_send(websocket, {"status": "error", "message": "Недействительный токен, выполните вход заново."})
            return

        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        
        # 1. Проверяем или создаем единый диалог для аккаунта
        await cursor.execute("SELECT id FROM dialogs WHERE user_id = %s", (user_id,))
        dialog = await cursor.fetchone()
        if not dialog:
            await cursor.execute("INSERT INTO dialogs (name, user_id) VALUES ('Основной диалог', %s)", (user_id,))
            dialog_id = cursor.lastrowid
        else:
            dialog_id = dialog['id']

        # 2. Обновляем/Создаем устройство
        await cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
        device = await cursor.fetchone()
        if device:
            await cursor.execute("UPDATE devices SET device_name = %s, is_online = TRUE, user_id = %s WHERE mac = %s", (device_name, user_id, mac))
            device_id = device['id']
        else:
            await cursor.execute("INSERT INTO devices (mac, device_name, is_online, user_id) VALUES (%s, %s, TRUE, %s)", (mac, device_name, user_id))
            device_id = cursor.lastrowid
            
        mac_to_websocket[mac] = websocket; ws_to_mac[websocket] = mac
        
        # 3. Загружаем ЕДИНУЮ историю аккаунта (по dialog_id)
        history = []
        await cursor.execute("""
            SELECT m.id, CASE WHEN m.send_type = 'Вы' THEN 'Вы' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender, m.text, m.created_at as time
            FROM messages m 
            LEFT JOIN devices d ON m.send_type COLLATE utf8mb4_general_ci = CAST(d.id AS CHAR) COLLATE utf8mb4_general_ci
            WHERE m.dialog_id = %s ORDER BY m.created_at ASC
        """, (dialog_id,))
        
        for msg in await cursor.fetchall():
            msg_time = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else msg['time']
            history.append({"id": msg['id'], "sender": msg['sender'], "text": msg['text'], "time": msg_time})
        
        await cursor.execute("SELECT login FROM users WHERE id = %s", (user_id,))
        user_rec = await cursor.fetchone()

        await conn.commit()
        await async_send(websocket, {
            "status": "success", 
            "message": "Данные успешно обработаны!", 
            "history": history,
            "user_login": user_rec['login'] if user_rec else "User"
        })
    except Exception as e:
        logger.error(f"[AUTH ERROR] Ошибка десктопной авторизации: {e}", exc_info=True)
        await async_send(websocket, {"status": "error", "message": str(e)})
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()