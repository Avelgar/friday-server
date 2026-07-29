import sys
sys.path.append('/opt/friday')

import asyncio
import logging
from app.database.connection import get_db_connection
from app.websocket_server.server import start_ws_server

logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("WS_Server")

def reset_online_statuses():
    try:
        startup_conn = get_db_connection()
        startup_cursor = startup_conn.cursor()
        startup_cursor.execute("UPDATE devices SET is_online = FALSE")
        startup_conn.commit()
        startup_conn.close()
        logger.info("Онлайн статусы устройств сброшены.")
    except Exception as e:
        logger.error(f"Ошибка сброса статусов: {e}")

if __name__ == '__main__':
    reset_online_statuses()
    asyncio.run(start_ws_server())