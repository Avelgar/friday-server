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
from app.websocket_server.handlers_cmds import handle_audio_chunk, handle_audio_end

logger = logging.getLogger("WS_Server")

def cleanup_disconnected_device(mac):
    """Вспомогательная функция для обновления статуса или удаления WEB-устройств"""
    if not mac:
        return
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
        dev = cur.fetchone()
        if dev:
            dev_id = dev[0]
            if str(mac).startswith("WEB"):
                # Удаляем мусорные веб-сессии и их сообщения
                cur.execute("DELETE FROM messages WHERE recipient_device_id = %s", (dev_id,))
                cur.execute("DELETE FROM devices WHERE id = %s", (dev_id,))
            else:
                # Нормальные устройства просто помечаем как оффлайн
                cur.execute("UPDATE devices SET is_online = FALSE WHERE id = %s", (dev_id,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка при очистке устройства {mac}: {e}")


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
                
                if data.get("type") == "ping": 
                    continue
                if "DeviceName" in data: 
                    await handle_device_registration(websocket, data)
                elif "command" in data: 
                    asyncio.create_task(handle_command(websocket, data))
                elif "command_to_device" in data: 
                    asyncio.create_task(handle_target_command(websocket, data))
                elif data.get("type") == "web_client_auth": 
                    await handle_web_client_auth(websocket, data)
                elif data.get("type") == "audio_stream_chunk": 
                    await handle_audio_chunk(websocket, data)
                elif data.get("type") == "audio_stream_end": 
                    await handle_audio_end(websocket, data)
            except Exception as e: 
                pass
    except ConnectionClosed: 
        pass
    except Exception as e: 
        pass
    finally:
        logger.info(f"Disconnected: {client_id}")
        if websocket in active_connections: del active_connections[websocket]
        if websocket in last_ping_times: del last_ping_times[websocket]
        
        mac = ws_to_mac.get(websocket)
        if mac:
            mac_to_websocket.pop(mac, None)
            ws_to_mac.pop(websocket, None)
            cleanup_disconnected_device(mac)

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
                    cleanup_disconnected_device(mac)
                try: await ws.close()
                except: pass
        except: pass
        await asyncio.sleep(10)

async def start_ws_server():
    asyncio.create_task(check_pings())
    async with websockets.serve(websocket_handler, "0.0.0.0", 8114, ping_interval=None, max_size=10 * 1024 * 1024):
        logger.info("WebSocket Server started on 8114")
        await asyncio.Future()