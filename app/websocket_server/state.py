# Хранилище активных сессий в оперативной памяти (без БД)

# ws -> client_id
active_connections = {}  

# ws -> {"user_id": int, "user_login": str, "device_name": str, "device_type": str}
ws_sessions = {}

# user_id -> set of ws (для быстрого поиска всех онлайн-устройств пользователя)
user_connections = {}

# ws -> timestamp последнего пинга
last_ping_times = {}     
PING_TIMEOUT = 70
