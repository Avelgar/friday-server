import json
import base64
import websockets
from app.websocket_server import state

DEVICE_TYPES_RU = {
    'pc': 'компьютер',
    'phone': 'телефон',
    'robot': 'робот',
    'web': 'браузер'
}

def get_device_type(type_str=None, mac=None):
    """Определение понятного русского названия типа устройства."""
    if type_str and str(type_str).lower() in DEVICE_TYPES_RU:
        return DEVICE_TYPES_RU[str(type_str).lower()]
    if mac:
        if '-' in str(mac): return "компьютер"
        if 'WEB' in str(mac): return "браузер"
        if str(mac).lower() == "b8:27:eb:00:51:06" or "pi" in str(mac).lower(): return "робот"
    return "устройство"

# Алиас для обратной совместимости
get_device_type_ru = get_device_type

async def get_accessible_devices(cursor_unused, current_ws, user_id):
    """
    Возвращает список всех онлайн-устройств пользователя прямо из оперативной памяти (без БД).
    """
    devices = []
    if not user_id:
        return devices

    user_ws_set = state.user_connections.get(user_id, set())
    for ws in user_ws_set:
        if ws == current_ws:
            continue
        session = state.ws_sessions.get(ws)
        if session:
            d_name = session.get('device_name', 'Устройство')
            d_type = get_device_type(type_str=session.get('device_type', 'pc'))
            devices.append(f"{d_name} ({d_type})")

    return devices

async def async_send(websocket, data):
    try:
        if websocket.state != websockets.protocol.State.OPEN:
            return
        json_data = json.dumps(data, ensure_ascii=False)
        encoded_data = base64.b64encode(json_data.encode('utf-8')).decode('utf-8')
        await websocket.send(encoded_data)
    except Exception:
        pass

async def broadcast_to_user(user_id, data, exclude_ws=None):
    """
    Мгновенная рассылка события по WebSocket всем онлайн-устройствам конкретного пользователя.
    """
    if not user_id:
        return
    user_ws_set = state.user_connections.get(user_id, set())
    for ws in list(user_ws_set):
        if ws == exclude_ws:
            continue
        try:
            await async_send(ws, data)
        except Exception:
            pass
