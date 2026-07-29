import time
import base64
import json
import asyncio
import logging
import websockets
from websockets.exceptions import ConnectionClosed
from app.database.connection import get_db_connection

from app.websocket_server.state import active_connections, mac_to_websocket, ws_to_mac, last_ping_times, PING_TIMEOUT
from app.websocket_server.handlers_auth import handle_device_registration, handle_web_client_auth
from app.websocket_server.handlers_cmds import handle_command, handle_target_command

logger = logging.getLogger("WS_Server")

async def websocket_handler(websocket):
    client_id = id(websocket)
    active_connections[websocket] = client_id
    last_ping_times[websocket] = time.time()
    logger.info(f"New connection: {client_id}")
    
    try:
        async for message in websocket:
            try:
                decoded = base64.b64decode(message).decode('utf-8').strip().replace('\0x00', '')
                data = json.loads(decoded)
                last_ping_times[websocket] = time.time()
                
                if data.get("type") == "ping": continue
                if "DeviceName" in data: await handle_device_registration(websocket, data)
                elif "command" in data: await handle_command(websocket, data)
                elif "command_to_device" in data: await handle_target_command(websocket, data)
                elif data.get("type") == "web_client_auth": await handle_web_client_auth(websocket, data)
            except Exception: pass
    except ConnectionClosed: pass
    except Exception: pass
    finally:
        logger.info(f"Disconnected: {client_id}")
        if websocket in active_connections: del active_connections[websocket]
        if websocket in last_ping_times: del last_ping_times[websocket]
        
        mac = ws_to_mac.get(websocket)
        if mac:
            mac_to_websocket.pop(mac, None)
            ws_to_mac.pop(websocket, None)
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("UPDATE devices SET is_online = FALSE WHERE mac = %s", (mac,))
                conn.commit(); conn.close()
            except: pass

async def check_pings():
    while True:
        try:
            now = time.time()
            to_remove = [ws for ws, last_time in list(last_ping_times.items()) if now - last_time > PING_TIMEOUT]
            for ws in to_remove:
                last_ping_times.pop(ws, None); active_connections.pop(ws, None)
                mac = ws_to_mac.get(ws)
                if mac:
                    mac_to_websocket.pop(mac, None); ws_to_mac.pop(ws, None)
                    try:
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute("UPDATE devices SET is_online = FALSE WHERE mac = %s", (mac,))
                        conn.commit(); conn.close()
                    except: pass
                try: await ws.close()
                except: pass
        except: pass
        await asyncio.sleep(10)

async def start_ws_server():
    asyncio.create_task(check_pings())
    async with websockets.serve(websocket_handler, "0.0.0.0", 8114, ping_interval=None, max_size=10 * 1024 * 1024):
        logger.info("WebSocket Server started on 8114")
        await asyncio.Future()