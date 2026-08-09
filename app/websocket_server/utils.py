import json
import base64
import websockets

def get_device_type(mac):
    if not mac: return "неизвестно"
    if '-' in mac: return "компьютер"
    if 'WEB' in mac: return "браузер"
    if mac == "b8:27:eb:00:51:06": return "распберри"
    return "телефон"

async def get_accessible_devices(cursor, current_mac, user_id):
    devices = {}
    if user_id:
        await cursor.execute("SELECT mac, device_name FROM devices WHERE user_id = %s AND mac != %s AND is_online = TRUE", (user_id, current_mac))
        for row in await cursor.fetchall():
            devices[row['mac']] = f"{row['device_name']} ({get_device_type(row['mac'])})"
            
    await cursor.execute("SELECT id FROM devices WHERE mac = %s", (current_mac,))
    dev = await cursor.fetchone()
    if dev:
        dev_id = dev['id']
        await cursor.execute("""
            SELECT d.mac, d.device_name FROM device_access da
            JOIN devices d ON da.owner_id = d.id WHERE da.guest_id = %s AND d.is_online = TRUE
        """, (dev_id,))
        for row in await cursor.fetchall():
            devices[row['mac']] = f"{row['device_name']} ({get_device_type(row['mac'])})"
                
        await cursor.execute("""
            SELECT d.mac, d.device_name FROM device_access da
            JOIN devices d ON da.guest_id = d.id WHERE da.owner_id = %s AND d.is_online = TRUE
        """, (dev_id,))
        for row in await cursor.fetchall():
            devices[row['mac']] = f"{row['device_name']} ({get_device_type(row['mac'])})"

    return list(devices.values())

async def async_send(websocket, data):
    try:
        if websocket.state != websockets.protocol.State.OPEN: return
        json_data = json.dumps(data, ensure_ascii=False)
        encoded_data = base64.b64encode(json_data.encode('utf-8')).decode('utf-8')
        await websocket.send(encoded_data)
    except Exception:
        pass