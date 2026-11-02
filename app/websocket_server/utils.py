import json
import base64
import websockets

DEVICE_TYPES_RU = {
    'pc': 'компьютер',
    'phone': 'телефон',
    'robot': 'робот',
    'web': 'браузер'
}

def get_device_type(mac=None, type_str=None):
    """Определение понятного русского названия типа устройства."""
    if type_str and type_str in DEVICE_TYPES_RU:
        return DEVICE_TYPES_RU[type_str]
    if not mac:
        return "устройство"
    if '-' in str(mac):
        return "компьютер"
    if 'WEB' in str(mac):
        return "браузер"
    if str(mac).lower() == "b8:27:eb:00:51:06" or "pi" in str(mac).lower():
        return "робот"
    return "телефон"

# Алиас для обратной совместимости
get_device_type_ru = get_device_type

async def get_accessible_devices(cursor, current_mac, user_id):
    """
    Возвращает список всех онлайн-устройств пользователя.
    Все свои устройства онлайн видят друг друга без таблицы device_access.
    """
    devices = []
    if not user_id:
        return devices

    await cursor.execute(
        """SELECT mac, device_name, device_type 
           FROM devices 
           WHERE user_id = %s AND mac != %s AND is_online = TRUE""",
        (user_id, current_mac)
    )
    rows = await cursor.fetchall()
    for row in rows:
        d_type = get_device_type(type_str=row.get('device_type', 'pc'), mac=row.get('mac'))
        devices.append(f"{row['device_name']} ({d_type})")

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