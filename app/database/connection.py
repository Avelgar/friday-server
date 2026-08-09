import mysql.connector
import aiomysql
from app.config.settings import DB_CONFIG

def get_db_connection():
    """Синхронное подключение (Оставлено для HTTP-сервера)"""
    return mysql.connector.connect(**DB_CONFIG)

async def get_async_db_connection():
    """Асинхронное подключение (Для WebSocket-сервера)"""
    config = DB_CONFIG.copy()
    # aiomysql требует ключ 'db' вместо 'database'
    if 'database' in config:
        config['db'] = config.pop('database')
    
    return await aiomysql.connect(**config, autocommit=False)