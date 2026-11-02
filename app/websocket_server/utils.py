import json
import base64
import websockets

DEVICE_TYPES_RU = {
    'pc': 'компьютер',
    'phone': 'телефон',
    'robot': 'робот',
    'web': 'браузер'
}

def get_device_type_ru(type_str):
    return DEVICE_TYPES_RU.get(type_str, 'устройство')

async def get_accessible_devices(cursor, current_mac, user_id):
    """
    Возвращает список всех онлайн-устройств пользователя.
    Больше никаких паролей и таблицы device_access — все свои устройства доверяют друг другу!
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
        d_type = get_device_type_ru(row.get('device_type', 'pc'))
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