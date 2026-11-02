import hashlib
import jwt
import logging
import aiomysql
from app.config.settings import JWT_SECRET
from app.database.connection import get_async_db_connection
from app.websocket_server.state import mac_to_websocket, ws_to_mac
from app.websocket_server.utils import async_send, get_device_type_ru

logger = logging.getLogger("WS_Server")

async def handle_auth(websocket, data):
    """
    Единая авторизация всех устройств по токену аккаунта.
    Никаких паролей железок!
    """
    conn = None; cursor = None
    try:
        token = data.get('token')
        mac = data.get('mac') or data.get('MAC')
        device_name = data.get('device_name') or data.get('DeviceName') or 'Компьютер'
        device_type = data.get('device_type', 'pc')

        # Если это веб-сайт
        if data.get('type') == 'web_client_auth' or str(mac).startswith('WEB'):
            device_type = 'web'
            if not mac:
                mac = f"WEB{hashlib.md5(str(token).encode()).hexdigest()[:13]}"
            device_name = f"Браузер {mac[-4:]}"

        if not token:
            logger.warning("[AUTH] Попытка входа без токена аккаунта.")
            await async_send(websocket, {"type": "auth_error", "status": "error", "message": "Токен аккаунта не предоставлен"})
            return

        # 1. Проверяем JWT токен аккаунта
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            user_id = payload['user_id']
        except Exception:
            logger.warning("[AUTH] Недействительный или просроченный JWT токен.")
            await async_send(websocket, {"type": "auth_error", "status": "error", "message": "Недействительный токен аккаунта"})
            return

        conn = await get_async_db_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)

        # 2. Проверяем, существует ли пользователь
        await cursor.execute("SELECT id, login FROM users WHERE id = %s", (user_id,))
        user_rec = await cursor.fetchone()
        if not user_rec:
            await async_send(websocket, {"type": "auth_error", "status": "error", "message": "Пользователь не найден"})
            return

        user_login = user_rec['login']

        # 3. Привязываем/обновляем устройство в таблице devices
        await cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
        existing_device = await cursor.fetchone()

        if existing_device:
            logger.info(f"[AUTH] Устройство '{device_name}' (MAC: {mac}) онлайн. Хозяин: {user_login}")
            await cursor.execute(
                """UPDATE devices 
                   SET device_name = %s, device_type = %s, user_id = %s, is_online = TRUE 
                   WHERE mac = %s""",
                (device_name, device_type, user_id, mac)
            )
            device_id = existing_device['id']
        else:
            logger.info(f"[AUTH] Регистрация НОВОГО устройства '{device_name}' ({device_type}, MAC: {mac}) для {user_login}")
            await cursor.execute(
                """INSERT INTO devices (mac, device_name, device_type, user_id, is_online) 
                   VALUES (%s, %s, %s, %s, TRUE)""",
                (mac, device_name, device_type, user_id)
            )
            device_id = cursor.lastrowid

        mac_to_websocket[mac] = websocket
        ws_to_mac[websocket] = mac

        # 4. Подтягиваем историю последнего диалога пользователя
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

        await conn.commit()

        # 5. Отдаем клиенту успешный ответ
        # "Данные успешно обработаны!" оставлено для совместимости со старыми проверками клиента
        await async_send(websocket, {
            "type": "account_sync_success",
            "status": "success",
            "message": "Данные успешно обработаны!",
            "user_login": user_login,
            "device_id": device_id,
            "dialog_id": dialog['id'] if dialog else None,
            "history": history
        })
        logger.info(f"[AUTH] ✅ {user_login} вошел с '{device_name}'. Устройство готово к работе.")

    except Exception as e:
        logger.error(f"[AUTH ERROR] {e}", exc_info=True)
        await async_send(websocket, {"type": "auth_error", "status": "error", "message": str(e)})
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()