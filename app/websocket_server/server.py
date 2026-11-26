import time
import base64
import json
import asyncio
import logging
import websockets
from websockets.exceptions import ConnectionClosed

from app.websocket_server import state
from app.websocket_server.utils import broadcast_to_user
from app.websocket_server.handlers_auth import handle_auth
from app.websocket_server.handlers_cmds import (
    handle_command,
    handle_audio_chunk,
    handle_video_chunk,
    handle_audio_end,
)
from app.websocket_server.handlers_antigravity_crud import (
    handle_antigravity_get_chats,
    handle_antigravity_get_messages,
    handle_antigravity_create_chat,
    handle_antigravity_delete_chat,
    handle_antigravity_register_client,
    handle_antigravity_delete_synced_file,
    handle_antigravity_save_system_log,
    handle_antigravity_list_server_files,
    handle_antigravity_delete_server_file,
    handle_antigravity_list_env_files,
    handle_antigravity_download_env_file,
    handle_antigravity_upload_env_file,
)
from app.websocket_server.handlers_antigravity_task import (
    handle_antigravity_cancel_task,
    handle_antigravity_task,
)

logger = logging.getLogger("WS_Server")


class InternalNotifyProtocol(asyncio.DatagramProtocol):
    """
    Внутренний межпроцессный протокол на loopback (127.0.0.1:8116).
    Позволяет HTTP-серверу (в отдельном процессе) мгновенно передавать
    события создания/удаления диалогов и сообщений для рассылки всем устройствам пользователя.
    """
    def datagram_received(self, data, addr):
        try:
            msg = json.loads(data.decode('utf-8'))
            user_id = msg.get("user_id")
            payload = msg.get("payload")
            if user_id and payload:
                asyncio.create_task(broadcast_to_user(user_id, payload))
        except Exception as e:
            logger.error(f"[IPC UDP] Ошибка обработки внутреннего уведомления: {e}")


def cleanup_connection(websocket):
    """Моментальная очистка сокета из памяти без обращений к базе данных."""
    state.active_connections.pop(websocket, None)
    state.last_ping_times.pop(websocket, None)
    session = state.ws_sessions.pop(websocket, None)
    if session:
        user_id = session.get('user_id')
        if user_id and user_id in state.user_connections:
            state.user_connections[user_id].discard(websocket)
            if not state.user_connections[user_id]:
                state.user_connections.pop(user_id, None)
        logger.info(f"[DISCONNECT] Устройство '{session.get('device_name')}' ({session.get('device_type')}) удалено из памяти.")


async def websocket_handler(websocket):
    client_id = id(websocket)
    state.active_connections[websocket] = client_id
    state.last_ping_times[websocket] = time.time()
    logger.info(f"New connection: {client_id}")

    try:
        async for message in websocket:
            try:
                decoded = base64.b64decode(message).decode('utf-8').strip().replace('\0x00', '')
                data = json.loads(decoded)
                state.last_ping_times[websocket] = time.time()

                msg_type = data.get("type")
                if msg_type == "ping":
                    continue
                elif msg_type in ("auth", "web_client_auth", "account_sync") or ("token" in data and "command" not in data and "command_to_device" not in data):
                    asyncio.create_task(handle_auth(websocket, data))
                elif msg_type == "get_chats":
                    asyncio.create_task(handle_antigravity_get_chats(websocket, data))
                elif msg_type == "get_chat_history":
                    asyncio.create_task(handle_antigravity_get_messages(websocket, data))
                elif msg_type == "create_chat":
                    asyncio.create_task(handle_antigravity_create_chat(websocket, data))
                elif msg_type == "delete_chat":
                    asyncio.create_task(handle_antigravity_delete_chat(websocket, data))
                elif msg_type == "register_client":
                    asyncio.create_task(handle_antigravity_register_client(websocket, data))
                elif msg_type == "delete_synced_file":
                    asyncio.create_task(handle_antigravity_delete_synced_file(websocket, data))
                elif msg_type == "save_system_log":
                    asyncio.create_task(handle_antigravity_save_system_log(websocket, data))
                elif msg_type == "list_server_files":
                    asyncio.create_task(handle_antigravity_list_server_files(websocket, data))
                elif msg_type == "delete_server_file":
                    asyncio.create_task(handle_antigravity_delete_server_file(websocket, data))
                elif msg_type == "list_env_files":
                    asyncio.create_task(handle_antigravity_list_env_files(websocket, data))
                elif msg_type == "download_env_file":
                    asyncio.create_task(handle_antigravity_download_env_file(websocket, data))
                elif msg_type == "upload_env_file":
                    asyncio.create_task(handle_antigravity_upload_env_file(websocket, data))
                elif msg_type == "cancel_task":
                    asyncio.create_task(handle_antigravity_cancel_task(websocket, data))
                elif msg_type == "antigravity_task":
                    asyncio.create_task(handle_antigravity_task(websocket, data))
                elif "command" in data or "command_to_device" in data:
                    asyncio.create_task(handle_command(websocket, data))
                elif msg_type == "video_stream_chunk":
                    logger.info("🎥 Получен кадр видео от клиента!")
                    await handle_video_chunk(websocket, data)
                elif msg_type == "audio_stream_chunk":
                    await handle_audio_chunk(websocket, data)
                elif msg_type == "audio_stream_end":
                    await handle_audio_end(websocket, data)
            except Exception:
                pass
    except ConnectionClosed:
        pass
    except Exception:
        pass
    finally:
        logger.info(f"Disconnected: {client_id}")
        cleanup_connection(websocket)


async def check_pings():
    while True:
        try:
            now = time.time()
            to_remove = [ws for ws, last_time in list(state.last_ping_times.items()) if now - last_time > state.PING_TIMEOUT]
            for ws in to_remove:
                cleanup_connection(ws)
                try:
                    await ws.close()
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(10)


async def start_ws_server():
    loop = asyncio.get_running_loop()
    try:
        await loop.create_datagram_endpoint(
            lambda: InternalNotifyProtocol(),
            local_addr=('127.0.0.1', 8116)
        )
        logger.info("Внутренний IPC UDP-слушатель запущен на 127.0.0.1:8116")
    except Exception as e:
        logger.error(f"Не удалось запустить IPC UDP-слушатель: {e}")

    asyncio.create_task(check_pings())
    async with websockets.serve(websocket_handler, "0.0.0.0", 8114, ping_interval=None, max_size=100 * 1024 * 1024):
        logger.info("WebSocket Server started on 8114 (100MB limit, RAM session management)")
        await asyncio.Future()