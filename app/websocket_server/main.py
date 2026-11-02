# -*- coding: utf-8 -*-
import sys
sys.path.append('/opt/friday')

import asyncio
import logging
from app.websocket_server.server import start_ws_server

logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("WS_Server")

async def main():
    logger.info("Запуск Friday WebSocket Server (RAM-режим сессий)...")
    await start_ws_server()

if __name__ == '__main__':
    asyncio.run(main())
