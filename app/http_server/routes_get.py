import os
import urllib.parse
import jwt
from app.config.settings import JWT_SECRET
from app.database.connection import get_db_connection

def do_GET(self):
    parsed_path = urllib.parse.urlparse(self.path)
    path = parsed_path.path
    query_params = urllib.parse.parse_qs(parsed_path.query)

    if path == '/': self.serve_file('index.html', 'text/html; charset=utf-8')
    elif path == '/style.css': self.serve_file('style.css', 'text/css; charset=utf-8')
    elif path == '/script.js': self.serve_file('script.js', 'application/javascript; charset=utf-8') 
    elif path == '/image': self.serve_file('image.html', 'text/html; charset=utf-8')
    elif path == '/images/f.png': self.serve_file('images/f.png', 'image/png')
    elif path == '/download-windows':
        self.send_response(302); self.send_header('Location', 'https://disk.yandex.ru/d/ye8Rn1WFa1C-Lg'); self.end_headers()
    elif path == '/download-android':
        self.serve_file('friday.apk', 'application/vnd.android.package-archive', 'friday.apk')
    elif self.path == '/yandex_f01241a1225bebed.html':
        try:
            content = b"<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"></head><body>Verification: f01241a1225bebed</body></html>"
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=UTF-8')
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return
        except: self.send_error(500); return
    elif path.startswith('/recovery'):
        token = query_params.get('token', [None])[0]
        if not token: return self.redirect('/?message=recovery_no_token')
        conn = get_db_connection()
        try:
            cursor = conn.cursor(dictionary=True, buffered=True)
            cursor.execute("SELECT email FROM users WHERE RecoveryToken = %s AND RecoveryTokenDelTime > NOW()", (token,))
            if not cursor.fetchone(): return self.redirect('/?message=recovery_invalid_token')
            self.serve_file('recovery.html', 'text/html; charset=utf-8')
        finally: conn.close()
    elif path == '/verify':
        token = query_params.get('token', [None])[0]
        if token:
            conn = get_db_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM users WHERE SingUpToken = %s AND SingUpTokenDelTime > NOW()", (token,))
                user = cursor.fetchone()
                if user:
                    cursor.execute("UPDATE users SET SingUpToken = NULL, SingUpTokenDelTime = NULL WHERE id = %s", (user[0],))
                    conn.commit()
                    self.redirect('/?message=email_verified')
                else: self.redirect('/?message=invalid_token')
            finally: conn.close()
        else: self.redirect('/?message=no_token')
    elif path.startswith('/verify_token'):
        token = query_params.get('token', [None])[0]
        if not token: return self.send_json(401, {"status": "error", "message": "Токен не предоставлен"})
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True, buffered=True)
            cursor.execute("SELECT login FROM users WHERE id = %s", (payload['user_id'],))
            user = cursor.fetchone()
            conn.close()
            if user: self.send_json(200, {"status": "success", "message": "Токен действителен", "user_login": user['login']})
            else: self.send_json(401, {"status": "error", "message": "Пользователь не найден"})
        except: self.send_json(401, {"status": "error", "message": "Неверный токен"})
    else: self.send_error(404)