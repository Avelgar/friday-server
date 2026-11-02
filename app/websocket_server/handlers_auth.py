import jwt
import logging
import aiomysql
from app.config.settings import JWT_SECRET
from app.database.connection import get_async_db_connection
from app.websocket_server import state
from app.websocket_server.utils import async_send

logger = logging.getLogger("WS_Server")

async def handle_auth(websocket, data):
    """
    Единая авторизация всех устройств по JWT-токену аккаунта.
    Устройства сохраняются исключительно в оперативной памяти (без БД).
    """
    conn = None
    cursor = None
    try:
        token = data.get('token')
        device_name = data.get('device_name') or data.get('DeviceName') or 'Компьютер'
        device_type = data.get('device_type') or 'pc'

        # Если это веб-клиент
        if data.get('type') == 'web_client_auth' or str(data.get('mac', '')).startswith('WEB'):
            device_type = 'web'
            if not data.get('device_name'):
                device_name = 'Браузер'

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

        # 2. Проверяем, существует ли пользователь в БД
        await cursor.execute("SELECT id, login FROM users WHERE id = %s", (user_id,))
        user_rec = await cursor.fetchone()
        if not user_rec:
            await async_send(websocket, {"type": "auth_error", "status": "error", "message": "Пользователь не найден"})
            return

        user_login = user_rec['login']

        # 3. Регистрируем устройство ТОЛЬКО в оперативной памяти (RAM)
        state.ws_sessions[websocket] = {
            "user_id": user_id,
            "user_login": user_login,
            "device_name": device_name,
            "device_type": device_type
        }
        state.user_connections.setdefault(user_id, set()).add(websocket)

        # 4. Подтягиваем историю последнего диалога пользователя
        await cursor.execute("SELECT id FROM dialogs WHERE user_id = %s ORDER BY created_at DESC LIMIT 1", (user_id,))
        dialog = await cursor.fetchone()
        
        history = []
        if dialog:
            await cursor.execute("""
                SELECT id, sender, device_name, text, created_at as time
                FROM messages
                WHERE dialog_id = %s ORDER BY created_at ASC
            """, (dialog['id'],))
            for msg in await cursor.fetchall():
                msg_time = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else msg['time']
                history.append({
                    "id": msg['id'],
                    "sender": msg['sender'],
                    "device_name": msg.get('device_name'),
                    "text": msg['text'],
                    "time": msg_time
                })

        # 5. Отдаем клиенту успешный ответ
        await async_send(websocket, {
            "type": "account_sync_success",
            "status": "success",
            "message": "Данные успешно обработаны!",
            "user_login": user_login,
            "dialog_id": dialog['id'] if dialog else None,
            "history": history
        })
        logger.info(f"[AUTH] ✅ {user_login} вошел с '{device_name}' ({device_type}). Сессия сохранена в RAM.")

    except Exception as e:
        logger.error(f"[AUTH ERROR] {e}", exc_info=True)
        await async_send(websocket, {"type": "auth_error", "status": "error", "message": str(e)})
    finally:
        if cursor: await cursor.close()
        if conn: conn.close()
