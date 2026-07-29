import time
from app.database.connection import get_db_connection

def clean_expired_tokens():
    last_web_cleanup = time.time()
    web_cleanup_interval = 86400  

    while True:
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET RecoveryToken = NULL, RecoveryTokenDelTime = NULL WHERE RecoveryToken IS NOT NULL AND RecoveryTokenDelTime < NOW()")
            cursor.execute("DELETE FROM users WHERE SingUpToken IS NOT NULL AND SingUpTokenDelTime < NOW()")
            conn.commit()
            
            current_time = time.time()
            if current_time - last_web_cleanup >= web_cleanup_interval:
                cursor.execute("SELECT id FROM devices WHERE mac LIKE 'WEB%' AND is_online = FALSE AND created_at < DATE_SUB(NOW(), INTERVAL 7 DAY)")
                devices_to_delete = cursor.fetchall()
                for device in devices_to_delete:
                    cursor.execute("DELETE FROM messages WHERE recipient_device_id = %s", (device[0],))
                    cursor.execute("DELETE FROM devices WHERE id = %s", (device[0],))
                conn.commit()
                last_web_cleanup = current_time
        except Exception:
            pass
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
            time.sleep(3600)