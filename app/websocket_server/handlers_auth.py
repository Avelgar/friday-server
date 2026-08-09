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
        await cursor.execute("""
            SELECT m.id, CASE WHEN m.send_type = 'Вы' THEN 'Вы' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender, m.text, m.created_at as time
            FROM messages m LEFT JOIN devices d ON m.send_type = CAST(d.id AS CHAR) AND m.send_type != 'Вы' AND m.send_type != 'Бот'
            WHERE m.recipient_device_id = %s ORDER BY m.created_at ASC
        """, (device_id,))
        for msg in await cursor.fetchall():
            msg_time = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else msg['time']
            history.append({"id": msg['id'], "sender": msg['sender'], "text": msg['text'], "time": msg_time})
        
        await conn.commit()
        await async_send(websocket, {"status": "success", "message": "Данные успешно обработаны!", "history": history})
    except Exception as e:
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
        await cursor.execute("""
            SELECT m.id, CASE WHEN m.send_type = 'Вы' THEN 'Вы' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender, m.text, m.created_at as time
            FROM messages m LEFT JOIN devices d ON m.send_type = CAST(d.id AS CHAR) AND m.send_type != 'Вы' AND m.send_type != 'Бот'
            WHERE m.recipient_device_id = %s ORDER BY m.created_at ASC
        """, (device_id,))
        for msg in await cursor.fetchall():
            msg_time = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else msg['time']
            history.append({"id": msg['id'], "sender": msg['sender'], "text": msg['text'], "time": msg_time})
        
        response["history"] = history
        await conn.commit()
        await async_send(websocket, response)
    except Exception as e:
        logger.error(f"Ошибка регистрации устройства: {e}")
        await async_send(websocket, {"status": "error", "message": str(e)})
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()