import time
import base64
import json
import asyncio
import logging
import websockets
from websockets.exceptions import ConnectionClosed
from app.database.connection import get_async_db_connection

from app.websocket_server.state import active_connections, mac_to_websocket, ws_to_mac, last_ping_times, PING_TIMEOUT
from app.websocket_server.handlers_auth import handle_device_registration, handle_web_client_auth, handle_account_sync
from app.websocket_server.handlers_cmds import handle_command, handle_target_command
from app.websocket_server.handlers_cmds import handle_audio_chunk, handle_audio_end

logger = logging.getLogger("WS_Server")

async def cleanup_disconnected_device(mac):
    if not mac: return
    try:
        conn = await get_async_db_connection()
        cursor = await conn.cursor()
        await cursor.execute("SELECT id, user_id FROM devices WHERE mac = %s", (mac,))
        dev = await cursor.fetchone()
        if dev:
            dev_id = dev[0]
            user_id = dev[1]
            
            # УДАЛЯЕМ ТОЛЬКО ГОСТЕЙ! Авторизованные WEB-устройства оставляем для сохранения истории.
            if str(mac).startswith("WEB") and not user_id:
                await cursor.execute("DELETE FROM messages WHERE recipient_device_id = %s", (dev_id,))
                await cursor.execute("DELETE FROM devices WHERE id = %s", (dev_id,))
            else:
                await cursor.execute("UPDATE devices SET is_online = FALSE WHERE id = %s", (dev_id,))
                
        await conn.commit()
        await cursor.close()
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
                elif data.get('type') == 'video_stream_chunk':
                    from app.websocket_server.handlers_cmds import handle_video_chunk
                    logger.info("🎥 Получен кадр видео от клиента!")
                    await handle_video_chunk(websocket, data)
                elif data.get("type") == "web_client_auth": 
                    await handle_web_client_auth(websocket, data)
                elif data.get("type") == "account_sync": 
                    asyncio.create_task(handle_account_sync(websocket, data))
                elif data.get("type") == "audio_stream_chunk": 
                    await handle_audio_chunk(websocket, data)
                elif data.get("type") == "audio_stream_end": 
                    await handle_audio_end(websocket, data)
            except Exception: 
                pass
    except ConnectionClosed: 
        pass
    except Exception: 
        pass
    finally:
        logger.info(f"Disconnected: {client_id}")
        if websocket in active_connections: del active_connections[websocket]
        if websocket in last_ping_times: del last_ping_times[websocket]
        
        mac = ws_to_mac.get(websocket)
        if mac:
            mac_to_websocket.pop(mac, None)
            ws_to_mac.pop(websocket, None)
            await cleanup_disconnected_device(mac)

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
                    await cleanup_disconnected_device(mac)
                try: await ws.close()
                except: pass
        except: pass
        await asyncio.sleep(10)

async def start_ws_server():
    asyncio.create_task(check_pings())
    async with websockets.serve(websocket_handler, "0.0.0.0", 8114, ping_interval=None, max_size=10 * 1024 * 1024):
        logger.info("WebSocket Server started on 8114")
        await asyncio.Future()