# Хранилище активных сессий в оперативной памяти (без БД)

# ws -> client_id
active_connections = {}  

# ws -> {"user_id": int, "user_login": str, "device_name": str, "device_type": str}
ws_sessions = {}

# user_id -> set of ws (для быстрого поиска всех онлайн-устройств пользователя)
user_connections = {}

# ws -> timestamp последнего пинга
last_ping_times = {}     

# Клиент пингует раз в 30с (app.js), но фоновая/замороженная вкладка и прокси с
# idle-timeout'ом легко дают паузы в 1-2 минуты. При 70с сервер рвал живой сокет
# прямо во время долгой задачи Antigravity (клиент видел «Disconnected»),
# и клиент тут же переподключался + повторял create_chat -> гонка в БД.
PING_TIMEOUT = 240
