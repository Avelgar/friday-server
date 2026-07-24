import sys
import os
import json
import threading
import time
import urllib.parse
import logging
import secrets
import hashlib
import jwt
import asyncio
import base64
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

sys.path.append('/opt/friday')

from app.config.settings import JWT_SECRET
from app.database.connection import get_db_connection
from app.services.ai_service import ai_instance
from app.utils.email_sender import send_email

logging.getLogger("websockets").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("WS_Server")

active_connections = {}  
id_to_websocket = {}     
last_ping_times = {}     
PING_TIMEOUT = 70

CAP_PC = "открытие ссылки (принимает полную ссылку URL), напечатать текст (принимает текст), нажать кнопку мыши (лкм/пкм/скм), переместить мышь (координаты X, Y), уведомление (принимает текст), музыка (включить/выключить/следующий/предыдущий), смена имени (принимает текст), смена голоса (принимает СТРОГО одно из имен: Aoede/Puck/Kore/Charon), очистка истории (любой текст), изменение громкости (число от 0 до 100), изменение яркости (число от 0 до 100)"
CAP_PHONE = "открытие ссылки (принимает полную ссылку URL), изменение громкости (число от 0 до 100), изменение яркости (число от 0 до 100), музыка (включить/выключить/следующий/предыдущий), очистка истории (любой текст), режим камеры (любой текст), выключить режим камеры (любой текст)"
CAP_EXEC = "открытие файла (принимает полный путь), завершение процесса (принимает точное имя)"

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
                cursor.execute("SELECT id FROM devices WHERE mac LIKE 'WEB%' AND (websocket_id IS NULL OR websocket_id = '') AND created_at < DATE_SUB(NOW(), INTERVAL 7 DAY)")
                devices_to_delete = cursor.fetchall()
                for device in devices_to_delete:
                    cursor.execute("DELETE FROM messages WHERE recipient_device_id = %s", (device[0],))
                    cursor.execute("DELETE FROM devices WHERE id = %s", (device[0],))
                conn.commit()
                last_web_cleanup = current_time
            
        except Exception as e:
            pass
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
            time.sleep(3600)

threading.Thread(target=clean_expired_tokens, daemon=True).start()

def get_device_type(mac):
    if not mac: return "неизвестно"
    if '-' in mac: return "компьютер"
    if 'WEB' in mac: return "браузер"
    if mac == "b8:27:eb:00:51:06": return "распберри"
    return "телефон"

def get_accessible_devices(cursor, current_mac, user_id):
    devices = {}
    if user_id:
        cursor.execute("SELECT mac, device_name FROM devices WHERE user_id = %s AND websocket_id IS NOT NULL AND mac != %s", (user_id, current_mac))
        for row in cursor.fetchall():
            devices[row['mac']] = f"{row['device_name']} ({get_device_type(row['mac'])})"
            
    cursor.execute("SELECT mac, device_name, access_list FROM devices WHERE websocket_id IS NOT NULL AND mac != %s", (current_mac,))
    for row in cursor.fetchall():
        target_mac = row['mac']
        target_al = row.get('access_list') or ''
        if current_mac in target_al:
            devices[target_mac] = f"{row['device_name']} ({get_device_type(target_mac)})"
            
    cursor.execute("SELECT access_list FROM devices WHERE mac = %s", (current_mac,))
    my_al_row = cursor.fetchone()
    if my_al_row and my_al_row.get('access_list'):
        my_macs = [m.strip() for m in my_al_row['access_list'].split(';') if m.strip()]
        if my_macs:
            placeholders = ','.join(['%s']*len(my_macs))
            cursor.execute(f"SELECT mac, device_name FROM devices WHERE mac IN ({placeholders}) AND websocket_id IS NOT NULL AND mac != %s", tuple(my_macs) + (current_mac,))
            for row in cursor.fetchall():
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

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    pass

class HTTPRequestHandler(BaseHTTPRequestHandler):
    
    def send_json(self, status_code, data):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def serve_file(self, filename, content_type, download_name=None):
        try:
            file_path = os.path.join('/opt/friday', filename)
            with open(file_path, 'rb') as f:
                self.send_response(200)
                self.send_header('Content-type', content_type)
                if download_name: self.send_header('Content-Disposition', f'attachment; filename="{download_name}"')
                self.end_headers()
                self.wfile.write(f.read())
        except FileNotFoundError:
            self.send_error(404, "File Not Found")

    def handle_one_request(self):
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if not self.raw_requestline:
                self.close_connection = True
                return

            try: self.requestline = self.raw_requestline.decode('utf-8', errors='ignore')[:100]
            except: self.requestline = str(self.raw_requestline[:100])

            if not self.raw_requestline.startswith((b'GET', b'POST', b'PUT', b'DELETE', b'HEAD', b'OPTIONS')):
                try:
                    data = json.loads(self.raw_requestline.decode('utf-8').strip())
                    self.requestline = "POST /raw_json HTTP/1.1"
                    self.command = "POST"
                    self.path = "/raw_json"
                    self.headers = {}
                    self.raw_data = data
                    self.do_POST()
                    return
                except:
                    self.send_error(400, "Bad Request")
                    return

            if not self.parse_request(): return

            method = 'do_' + self.command
            if hasattr(self, method): getattr(self, method)()
            else: self.send_error(501, "Unsupported method")
                
        except Exception as e:
            logger.error(f"Error handling request: {e}")
            self.close_connection = True

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        path = parsed_path.path
        query_params = urllib.parse.parse_qs(parsed_path.query)

        if path == '/': self.serve_file('index.html', 'text/html; charset=utf-8')
        elif path == '/style.css': self.serve_file('style.css', 'text/css; charset=utf-8')
        elif path == '/image': self.serve_file('image.html', 'text/html; charset=utf-8')
        elif path == '/images/f.png': self.serve_file('images/f.png', 'image/png')
        elif path == '/download-windows':
            self.send_response(302); self.send_header('Location', 'https://disk.yandex.ru/d/ye8Rn1WFa1C-Lg'); self.end_headers()
        elif path == '/download-android':
            self.serve_file('friday.apk', 'application/vnd.android.package-archive', 'friday.apk')
        elif self.path == '/yandex_f01241a1225bebed.html':
          try:
              self.send_response(200); self.send_header('Content-Type', 'text/html; charset=UTF-8'); self.end_headers()
              self.wfile.write(b"<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"></head><body>Verification: f01241a1225bebed</body></html>")
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

    def do_POST(self):
        if hasattr(self, 'raw_data'):
            data = self.raw_data
        else:
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 0:
                    post_data = self.rfile.read(content_length)
                    data = json.loads(post_data.decode('utf-8'))
                else:
                    data = {}
            except:
                return self.send_error(400, "Invalid JSON")

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        
        try:
            if self.path == '/generate':
                prompt = data.get('prompt', '')
                audio_base64 = data.get('audio_base64')
                voice_type = data.get('voice_type', 'Aoede')
                bot_name = data.get('bot_name', 'Пятница')
                command_type = data.get('command_type', 'текстовое сообщение')
                screenshot_base64 = data.get('screenshot')
                message_history = data.get('message_history', [])

                if not prompt and not audio_base64 and not screenshot_base64:
                    return self.send_json(400, {"status": "error", "message": "Пустой запрос"})

                history_text = ""
                if message_history:
                    history_text = "\n\nИСТОРИЯ СООБЩЕНИЙ ГОСТЯ:\n"
                    for msg in message_history:
                        role = "Пользователь" if msg.get('role') == 'user' else "Ассистент"
                        content = msg.get('content', '')[:500]
                        history_text += f"{role}: {content}\n"

                system_instruction = f"""Ты — ИИ-помощник {bot_name}. Твой собеседник находится на ВЕБ-САЙТЕ.
ПРАВИЛА УПРАВЛЕНИЯ САЙТОМ:
1. Ты общаешься ТОЛЬКО ГОЛОСОМ. Говори естественно и живо.
2. Твой голос АВТОМАТИЧЕСКИ транслируется пользователю на сайт. Не используй "голосовой ответ" как action_type.
3. Доступные команды управления сайтом: очистка истории (любой текст), смена имени (принимает текст). 
4. Если пользователь просит сменить имя, вызови action_type="смена имени" и action_value="Новое Имя".
{history_text}
"""
                async def run_ai():
                    user_text = ""
                    bot_text = ""
                    audio_data = bytearray()
                    extracted_commands = []
                    
                    audio_bytes = base64.b64decode(audio_base64) if audio_base64 else None
                    image_bytes = base64.b64decode(screenshot_base64) if screenshot_base64 else None
                    prompt_formatted = f"[ЗАПРОС С САЙТА]: {prompt}" if prompt else ""

                    async for chunk in ai_instance.generate_audio_stream(
                        prompt_text=prompt_formatted,
                        system_instruction=system_instruction,
                        audio_bytes=audio_bytes,
                        image_bytes=image_bytes,
                        history_text="", 
                        voice_name=voice_type,
                        assistant_name=bot_name
                    ):
                        if chunk["type"] == "user_text":
                            user_text += chunk["text"] + " "
                        elif chunk["type"] == "bot_text":
                            bot_text += chunk["text"] + " "
                        elif chunk["type"] == "audio":
                            audio_data.extend(chunk["data"])
                        elif chunk["type"] == "commands":
                            extracted_commands.extend(chunk["commands"])
                            
                    return user_text.strip(), bot_text.strip(), audio_data, extracted_commands

                try:
                    user_text, bot_text, audio_data, ai_commands = asyncio.run(run_ai())
                except Exception as ex:
                    logger.error(f"Генерация HTTP провалилась: {ex}")
                    return self.send_json(500, {"status": "error", "message": f"Ошибка ИИ: {str(ex)}"})

                actions = []
                if bot_text: actions.append({"type": "текстовой ответ", "content": bot_text})
                    
                for cmd in ai_commands:
                    for act in cmd.get('actions', []):
                        actions.append({"type": act.get('action_type', ''), "content": act.get('action_value', '')})

                response_data = {
                    "status": "success", 
                    "user_text": user_text, # Возвращаем транскрибацию
                    "actions": actions,
                    "audio_base64": base64.b64encode(audio_data).decode('utf-8') if audio_data else None
                }
                
                self.send_json(200, response_data)

            # ... ОСТАЛЬНЫЕ РОУТЫ БЕЗ ИЗМЕНЕНИЙ ...
            elif self.path == '/api/generate_image':
                prompt = data.get('prompt')
                if not prompt: return self.send_json(400, {"status": "error", "message": "Промпт не может быть пустым"})
                try:
                    image_base64 = ai_instance.generate_image(prompt)
                    self.send_json(200, {"status": "success", "image_base64": image_base64, "message": "Изображение успешно сгенерировано"})
                except Exception as ex:
                    self.send_json(500, {"status": "error", "message": str(ex)})

            elif self.path == '/register':
                email = data.get('email'); login = data.get('login'); password = data.get('password')
                if not all([email, login, password]): return self.send_json(400, {"status": "error", "message": "Все поля обязательны"})
                cursor.execute("SELECT email, login FROM users WHERE email = %s OR login = %s", (email, login))
                existing = cursor.fetchall()
                if existing:
                    msg = "Пользователь уже существует"
                    if any(u['email'] == email for u in existing): msg = "Email уже занят"
                    elif any(u['login'] == login for u in existing): msg = "Логин уже занят"
                    return self.send_json(400, {"status": "error", "message": msg})
                token = secrets.token_urlsafe(32)
                link = f"https://friday-assistant.ru/verify?token={token}"
                if send_email(email, "Подтверждение регистрации", f"Ссылка: {link}"):
                    cursor.execute("INSERT INTO users (email, login, password, SingUpToken, SingUpTokenDelTime) VALUES (%s, %s, %s, %s, NOW() + INTERVAL 1 DAY)", (email, login, password, token))
                    conn.commit()
                    self.send_json(201, {"status": "success", "message": "Письмо отправлено"})
                else: self.send_json(500, {"status": "error", "message": "Ошибка отправки письма"})

            elif self.path == '/login':
                login = data.get('login'); password = data.get('password'); mac = data.get('mac')
                cursor.execute("SELECT * FROM users WHERE email = %s OR login = %s", (login, login))
                user = cursor.fetchone()
                if not user or user['password'] != password: return self.send_json(401, {"status": "error", "message": "Неверный логин или пароль"})
                if user['SingUpToken']: return self.send_json(403, {"status": "error", "message": "Аккаунт не подтвержден"})
                device_info = {'user_login': user['login']}
                if mac:
                    cursor.execute("UPDATE devices SET user_id = %s WHERE mac = %s", (user['id'], mac)); conn.commit()
                    cursor.execute("SELECT d.*, u.login as user_login FROM devices d LEFT JOIN users u ON d.user_id = u.id WHERE d.mac = %s", (mac,))
                    device_info = cursor.fetchone() or device_info
                self.send_json(200, {"status": "success", "message": "Вход выполнен", "user_login": device_info.get('user_login')})

            elif self.path == '/login_web':
                login = data.get('login'); password = data.get('password')
                cursor.execute("SELECT * FROM users WHERE email = %s OR login = %s", (login, login))
                user = cursor.fetchone()
                if not user or user['password'] != password: return self.send_json(401, {"status": "error", "message": "Неверный логин или пароль"})
                if user['SingUpToken']: return self.send_json(403, {"status": "error", "message": "Аккаунт не подтвержден"})
                token = jwt.encode({'user_id': user['id'], 'exp': datetime.utcnow() + timedelta(days=7)}, JWT_SECRET, algorithm='HS256')
                if isinstance(token, bytes): token = token.decode('utf-8')
                self.send_json(200, {"status": "success", "message": "Вход выполнен", "user_login": user['login'], "token": token})

            elif self.path == '/logout_web':
                token = data.get('token')
                if token:
                    mac = f"WEB{hashlib.md5(str(token).encode()).hexdigest()[:13]}"
                    cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
                    dev = cursor.fetchone()
                    if dev:
                        cursor.execute("DELETE FROM messages WHERE recipient_device_id = %s", (dev['id'],))
                        cursor.execute("DELETE FROM devices WHERE id = %s", (dev['id'],))
                        conn.commit()
                        return self.send_json(200, {"status": "success", "message": "Выход выполнен"})
                self.send_json(404, {"status": "error", "message": "Устройство не найдено"})

            elif self.path == '/logout':
                mac = data.get('MAC')
                if mac:
                    cursor.execute("UPDATE devices SET user_id = NULL WHERE mac = %s", (mac,))
                    if cursor.rowcount > 0:
                        conn.commit()
                        self.send_json(200, {"status": "success"})
                    else: self.send_json(404, {"status": "error", "message": "Устройство не найдено"})
                else: self.send_json(400, {"status": "error", "message": "MAC required"})

            elif self.path == '/recover-password':
                email = data.get('email')
                cursor.execute("SELECT id, SingUpToken, RecoveryToken FROM users WHERE email = %s", (email,))
                user = cursor.fetchone()
                if not user: return self.send_json(404, {"status": "error", "message": "Пользователь не найден"})
                if user['SingUpToken']: return self.send_json(400, {"status": "error", "message": "Аккаунт не подтвержден"})
                if user['RecoveryToken']: return self.send_json(400, {"status": "error", "message": "Восстановление уже запущено"})
                token = secrets.token_urlsafe(32)
                link = f"https://friday-assistant.ru/recovery?token={token}"
                if send_email(email, "Восстановление пароля", f"Ссылка: {link}"):
                    cursor.execute("UPDATE users SET RecoveryToken = %s, RecoveryTokenDelTime = NOW() + INTERVAL 1 HOUR WHERE id = %s", (token, user['id']))
                    conn.commit()
                    self.send_json(200, {"status": "success", "message": "Письмо отправлено"})
                else: self.send_json(500, {"status": "error", "message": "Ошибка отправки"})

            elif self.path == '/update-password':
                token = data.get('token'); password = data.get('password'); conf_pass = data.get('confirmPassword')
                if password != conf_pass: return self.send_json(400, {"status": "error", "message": "Пароли не совпадают"})
                cursor.execute("SELECT id FROM users WHERE RecoveryToken = %s AND RecoveryTokenDelTime > NOW()", (token,))
                user = cursor.fetchone()
                if user:
                    cursor.execute("UPDATE users SET password = %s, RecoveryToken = NULL, RecoveryTokenDelTime = NULL WHERE id = %s", (password, user['id']))
                    cursor.execute("UPDATE devices SET user_id = NULL WHERE user_id = %s", (user['id'],))
                    conn.commit()
                    self.send_json(200, {"status": "success", "message": "Пароль изменен"})
                else: self.send_json(400, {"status": "error", "message": "Неверный токен"})

            elif self.path == '/get_devices':
                mac = data.get('mac')
                cursor.execute("SELECT user_id, access_list FROM devices WHERE mac = %s", (mac,))
                device = cursor.fetchone()
                if not device: return self.send_json(404, {"status": "error", "message": "Устройство не найдено"})
                account_devices = []; my_devices = []; processed_macs = {mac}
                if device['user_id']:
                    cursor.execute("SELECT mac, device_name, (websocket_id IS NOT NULL) as is_online FROM devices WHERE user_id = %s AND mac != %s", (device['user_id'], mac))
                    for d in cursor.fetchall():
                        account_devices.append({"DeviceName": d['device_name'], "MacAddress": d['mac'], "IsOnline": bool(d['is_online']), "IsAccountDevice": True})
                        processed_macs.add(d['mac'])
                access_list = device['access_list'] or ''
                access_macs = [m.strip() for m in access_list.split(';') if m.strip() and m.strip() not in processed_macs]
                if access_macs:
                    ph = ','.join(['%s'] * len(access_macs))
                    cursor.execute(f"SELECT mac, device_name, (websocket_id IS NOT NULL) as is_online FROM devices WHERE mac IN ({ph})", tuple(access_macs))
                    for d in cursor.fetchall():
                        my_devices.append({"DeviceName": d['device_name'], "MacAddress": d['mac'], "IsOnline": bool(d['is_online']), "IsAccountDevice": False})
                self.send_json(200, {"status": "success", "account_devices": account_devices, "my_devices": my_devices})

            elif self.path == '/connect_device':
                req_mac = data.get('MAC'); dev_name = data.get('DeviceName'); pwd = data.get('Password')
                cursor.execute("SELECT mac, password, access_list FROM devices WHERE device_name = %s", (dev_name,))
                target = cursor.fetchone()
                cursor.execute("SELECT access_list FROM devices WHERE mac = %s", (req_mac,))
                requester = cursor.fetchone()
                if not target: return self.send_json(404, {"status": "error", "message": "Устройство не найдено"})
                if target['password'] != pwd: return self.send_json(401, {"status": "error", "message": "Неверный пароль"})
                if req_mac == target['mac']: return self.send_json(400, {"status": "error", "message": "Само к себе"})
                if not requester: return self.send_json(404, {"status": "error", "message": "Инициатор не найден"})
                def add_mac(alist, mac):
                    parts = [p for p in (alist or '').split(';') if p]
                    if mac not in parts: parts.append(mac)
                    return ';'.join(parts) + ';'
                new_target_list = add_mac(target['access_list'], req_mac)
                new_req_list = add_mac(requester['access_list'], target['mac'])
                cursor.execute("UPDATE devices SET access_list = %s WHERE mac = %s", (new_target_list, target['mac']))
                cursor.execute("UPDATE devices SET access_list = %s WHERE mac = %s", (new_req_list, req_mac))
                conn.commit()
                self.send_json(200, {"status": "success", "message": "Подключено", "target_mac": target['mac'], "target_device_name": dev_name})

            elif self.path == '/disconnect_device':
                req_mac = data.get('requester_mac'); target_mac = data.get('target_mac')
                cursor.execute("SELECT mac, access_list FROM devices WHERE mac IN (%s, %s)", (req_mac, target_mac))
                devs = {r['mac']: r for r in cursor.fetchall()}
                if len(devs) != 2: return self.send_json(404, {"status": "error", "message": "Устройства не найдены"})
                def remove_mac(alist, mac):
                    parts = [p for p in (alist or '').split(';') if p and p != mac]
                    return ';'.join(parts) + ';' if parts else ''
                new_req = remove_mac(devs[req_mac]['access_list'], target_mac)
                new_tar = remove_mac(devs[target_mac]['access_list'], req_mac)
                cursor.execute("UPDATE devices SET access_list = %s WHERE mac = %s", (new_req, req_mac))
                cursor.execute("UPDATE devices SET access_list = %s WHERE mac = %s", (new_tar, target_mac))
                conn.commit()
                self.send_json(200, {"status": "success", "message": "Отключено", "requester_new_list": new_req, "target_new_list": new_tar})

            elif self.path == '/clear_history':
                token = data.get('token'); mac = data.get('mac')
                if token: mac = f"WEB{hashlib.md5(str(token).encode()).hexdigest()[:13]}"
                cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
                dev = cursor.fetchone()
                if dev:
                    cursor.execute("DELETE FROM messages WHERE recipient_device_id = %s", (dev['id'],))
                    conn.commit()
                    self.send_json(200, {"status": "success", "message": "История очищена"})
                else: self.send_json(404, {"status": "error", "message": "Устройство не найдено"})
                
            elif self.path == '/delete_message':
                msg_id = data.get('msg_id'); token = data.get('token'); mac = data.get('mac')
                if token: mac = f"WEB{hashlib.md5(str(token).encode()).hexdigest()[:13]}"
                if not msg_id or not mac: return self.send_json(400, {"status": "error", "message": "msg_id и mac обязательны"})
                cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
                dev = cursor.fetchone()
                if not dev: return self.send_json(404, {"status": "error", "message": "Устройство не найдено"})
                cursor.execute("DELETE FROM messages WHERE id = %s AND recipient_device_id = %s", (msg_id, dev['id']))
                conn.commit()
                self.send_json(200, {"status": "success", "message": "Сообщение удалено"})

            elif self.path == '/edit_message':
                msg_id = data.get('msg_id'); new_text = data.get('new_text'); token = data.get('token'); mac = data.get('mac')
                if token: mac = f"WEB{hashlib.md5(str(token).encode()).hexdigest()[:13]}"
                if not msg_id or not new_text or not mac: return self.send_json(400, {"status": "error", "message": "msg_id, new_text и mac обязательны"})
                cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
                dev = cursor.fetchone()
                if not dev: return self.send_json(404, {"status": "error", "message": "Устройство не найдено"})
                cursor.execute("UPDATE messages SET text = %s WHERE id = %s AND recipient_device_id = %s", (new_text, msg_id, dev['id']))
                conn.commit()
                self.send_json(200, {"status": "success", "message": "Сообщение обновлено"})
                
            else:
                self.send_error(404)

        except Exception as e:
            logger.error(f"Error in POST {self.path}: {e}")
            self.send_json(500, {"status": "error", "message": str(e)})
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def redirect(self, url):
        self.send_response(302); self.send_header('Location', url); self.end_headers()

# === АВТОРИЗОВАННЫЙ ВЕБСОКЕТ (Полностью из твоего рабочего main.py, просто добавил сюда) ===
async def handle_command(websocket, data):
    conn = None
    cursor = None
    user_msg_id = None
    bot_message_id = None
    audio_chunks_count = 0
    has_commands = False
    final_user_text_full = ""
    final_bot_text_full = ""

    try:
        command = data.get('command', '[Пользователь отправил аудиосообщение]')
        timestamp_str = data.get('timestamp')
        name = data.get('name', 'Пятница')
        voice_name = data.get('voice_type', 'Aoede')
        screenshot_base64 = data.get('screenshot')
        audio_base64 = data.get('audio_base64') 
        mac = data.get('mac')
        ui_msg_id = data.get('ui_msg_id')
        
        image_bytes = base64.b64decode(screenshot_base64) if screenshot_base64 else None
        audio_bytes = base64.b64decode(audio_base64) if audio_base64 else None

        mysql_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        
        cursor.execute("SELECT * FROM devices WHERE websocket_id = %s", (id(websocket),))
        sender_device = cursor.fetchone()
        if not sender_device and mac:
            cursor.execute("SELECT * FROM devices WHERE mac = %s", (mac,))
            sender_device = cursor.fetchone()
        if not sender_device: raise Exception("Устройство отправителя не найдено")
        
        sender_id = sender_device['id']
        sender_name = sender_device['device_name']
        device_type = get_device_type(mac)

        db_user_placeholder = "🎤 [Слушаю...]" if audio_bytes else (command if command else "🖼️ [Фото]")
        
        logger.info("\n" + "="*50)
        logger.info(f"[REQUEST] ПЕРВИЧНЫЙ АГЕНТ. Инициатор: {sender_name}")

        cursor.execute("INSERT INTO messages (send_type, text, time, recipient_device_id) VALUES ('Вы', %s, %s, %s)", (db_user_placeholder, mysql_time, sender_id))
        conn.commit()
        user_msg_id = cursor.lastrowid

        cursor.execute("INSERT INTO messages (send_type, text, time, recipient_device_id) VALUES ('Бот', '', %s, %s)", (mysql_time, sender_id))
        conn.commit()
        bot_message_id = cursor.lastrowid

        cursor.execute("""
            SELECT CASE WHEN m.send_type = 'Вы' THEN 'Пользователь' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender_name, m.text
            FROM messages m LEFT JOIN devices d ON m.send_type = CAST(d.id AS CHAR) AND m.send_type NOT IN ('Вы', 'Бот')
            WHERE m.recipient_device_id = %s AND m.id < %s ORDER BY m.time ASC
        """, (sender_id, user_msg_id))
        history_for_prompt = "\n".join([f"{msg['sender_name']}: {msg['text']}" for msg in cursor.fetchall()])

        system_instruction = f"""Ты — ИИ-помощник {name}. Твой собеседник работает за устройством: {sender_name} (Тип: {device_type}).
ПРАВИЛА УПРАВЛЕНИЯ:
1. Говори естественно и живо. Твой голос сам транслируется пользователю.
2. Твои возможности тут: {CAP_PC if device_type == 'компьютер' else CAP_PHONE}.
3. Для сайтов/видео используй СТРОГО action_type="открытие ссылки" и валидную ссылку. НИКОГДА не обрезай ссылки!
4. Ты не знаешь путей. Если просят запустить программу, сначала вызови action_type="get_installed_programs".
5. Если просят сделать что-то на ДРУГОМ устройстве, используй action_type="check_network_devices".
ИСТОРИЯ ДИАЛОГА:
{history_for_prompt}
"""
        prompt = f"[СИСТЕМНЫЕ ДАННЫЕ]\nУстройство: {sender_name}\n[ЗАПРОС]: {command}"

        async for chunk in ai_instance.generate_audio_stream(
            prompt_text=prompt, 
            system_instruction=system_instruction,
            audio_bytes=audio_bytes,
            image_bytes=image_bytes, 
            history_text=history_for_prompt, 
            voice_name=voice_name, 
            assistant_name=name
        ):
            if chunk["type"] == "user_text":
                final_user_text_full += chunk["text"] + " "
                cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_user_text_full.strip(), user_msg_id))
                conn.commit()
                if sender_device['websocket_id']:
                    sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
                    if sender_ws: await async_send(sender_ws, {"type": "user_transcription", "ui_msg_id": ui_msg_id, "text": final_user_text_full.strip()})

            elif chunk["type"] == "bot_text":
                final_bot_text_full += chunk["text"] + " "
                cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_bot_text_full.strip(), bot_message_id))
                conn.commit()
                if sender_device['websocket_id']:
                    sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
                    if sender_ws: await async_send(sender_ws, {"type": "new_message", "message_id": bot_message_id, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": chunk["text"], "actions": []})

            elif chunk["type"] == "commands":
                if chunk["commands"]: has_commands = True
                extracted_commands = chunk["commands"]
                filtered_commands = []
                
                for cmd in extracted_commands:
                    filtered_actions = []
                    for act in cmd.get('actions', []):
                        if act.get('action_type') == "check_network_devices":
                            pseudo_data = {
                                "internal_routing": "check_network_devices",
                                "original_command": final_user_text_full.strip() or command,
                                "source_name": sender_name,
                                "mac": mac,
                                "user_id": sender_device.get('user_id'),
                                "user_msg_id": user_msg_id,
                                "voice_type": voice_name
                            }
                            asyncio.create_task(handle_target_command(websocket, pseudo_data))
                        else:
                            filtered_actions.append(act)
                            
                    if filtered_actions:
                        cmd['actions'] = filtered_actions
                        filtered_commands.append(cmd)
                
                for cmd in filtered_commands:
                    target_device_name = cmd.get('target_device', '').strip()
                    actions = cmd.get('actions', [])
                    if not target_device_name or not actions: continue
                    
                    cursor.execute("SELECT * FROM devices WHERE device_name = %s", (target_device_name,))
                    target_device_info = cursor.fetchone()
                    if not target_device_info:
                        cursor.execute("SELECT * FROM devices WHERE websocket_id IS NOT NULL")
                        for d in cursor.fetchall():
                            if d['device_name'].lower() in target_device_name.lower() or target_device_name.lower() in d['device_name'].lower():
                                target_device_info = d; break
                    if not target_device_info: continue

                    target_id = target_device_info['id']
                    is_sender = (target_id == sender_id)
                    device_spoken_text = " ".join([a.get('action_value', '') for a in actions if a.get('action_type') in ["голосовой ответ", "текстовой ответ"]])

                    target_audio_base64 = await ai_instance.generate_static_audio(device_spoken_text.strip(), voice_name, name) if (not is_sender and device_spoken_text.strip()) else None

                    if target_device_info['websocket_id']:
                        target_ws = id_to_websocket.get(int(target_device_info['websocket_id']))
                        if target_ws:
                            msg_id = bot_message_id if is_sender else None
                            if not is_sender and device_spoken_text:
                                cursor.execute("INSERT INTO messages (send_type, text, time, recipient_device_id) VALUES (%s, %s, %s, %s)", (str(sender_id), device_spoken_text.strip(), mysql_time, target_id))
                                msg_id = cursor.lastrowid; conn.commit()
                            
                            await async_send(target_ws, {
                                "type": "new_message",
                                "message_id": msg_id,
                                "ui_msg_id": ui_msg_id,
                                "sender": "Бот" if is_sender else sender_name,
                                "text": device_spoken_text.strip(), 
                                "actions": actions,
                                "audio_base64": target_audio_base64,
                                "source_device": sender_name,
                                "original_command": final_user_text_full.strip() or command
                            })

            elif chunk["type"] == "audio":
                audio_chunks_count += 1
                if sender_device['websocket_id']:
                    sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
                    if sender_ws: await async_send(sender_ws, {"type": "audio_chunk", "audio_base64": base64.b64encode(chunk["data"]).decode('utf-8')})
        
        if not final_bot_text_full.strip() and audio_chunks_count == 0 and not has_commands:
            cursor.execute("DELETE FROM messages WHERE id IN (%s, %s)", (bot_message_id, user_msg_id))
            conn.commit()
            if sender_device['websocket_id']:
                sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
                if sender_ws:
                    await async_send(sender_ws, {"type": "delete_message", "ui_msg_id": ui_msg_id})
                    await async_send(sender_ws, {"type": "new_message", "message_id": None, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": "", "actions": []})
        else:
            cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_bot_text_full.strip(), bot_message_id))
            conn.commit()
            if sender_device['websocket_id']:
                sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
                if sender_ws: await async_send(sender_ws, {"type": "new_message", "message_id": bot_message_id, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": "", "actions": []})

        logger.info(f"[DONE] Первичный цикл завершен.\n" + "="*50)

    except Exception as e:
        logger.error(f"[ERROR] {e}", exc_info=True)
        try:
            if sender_device and sender_device['websocket_id']:
                sender_ws = id_to_websocket.get(int(sender_device['websocket_id']))
                if sender_ws:
                    cursor.execute("DELETE FROM messages WHERE id IN (%s, %s)", (bot_message_id, user_msg_id))
                    conn.commit()
                    await async_send(sender_ws, {"type": "delete_message", "ui_msg_id": ui_msg_id})
                    await async_send(sender_ws, {"type": "new_message", "message_id": None, "ui_msg_id": ui_msg_id, "sender": "Бот", "text": "", "actions": []})
        except: pass

async def handle_target_command(websocket, data):
    conn = None
    cursor = None
    audio_chunks_count = 0
    has_commands = False
    try:
        mysql_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        
        is_internal = data.get("internal_routing")
        voice_name = data.get('voice_type', 'Aoede')
        name = data.get('name', 'Пятница')

        if is_internal == "check_network_devices":
            source_name = data.get("source_name")
            original_command = data.get("original_command")
            mac = data.get("mac")
            user_id = data.get("user_id")
            user_msg_id = data.get("user_msg_id")
            
            cursor.execute("SELECT * FROM devices WHERE device_name = %s", (source_name,))
            source_device_info = cursor.fetchone()
            sender_device = source_device_info 
            
            accessible_devices_list = get_accessible_devices(cursor, mac, user_id)
            accessible_devices = ", ".join(accessible_devices_list) if accessible_devices_list else "нет устройств в сети"
            
            system_instruction = f"""Ты — ИИ-помощник {name}. РОЛЬ: Сетевой Маршрутизатор.
Пользователь с устройства {source_name} попросил: "{original_command}".
Доступные устройства в сети: {accessible_devices}.
ПРАВИЛА:
1. Ответь пользователю на {source_name} живо и естественно. Если нужного устройства НЕТ в сети — просто скажи об этом.
3. ВНИМАНИЕ: Для открытия ссылок/видео на удаленном устройстве отправляй СТРОГО action_type="открытие ссылки" и полную валидную ссылку.
4. Если нужно открыть файл/программу, отправляй action_type="get_installed_programs". Если закрыть — action_type="get_running_processes".
"""
            prompt_context = "[СИСТЕМНОЕ ЗАДАНИЕ] Проверь наличие устройства в сети и маршрутизируй запрос, обязательно ответив пользователю."

        else:
            command = data.get('command_to_device')
            processes = data.get('processes', '')
            programs = data.get("programs", [])
            source_name = data.get('source_name') 
            original_command = command
            user_msg_id = data.get('user_msg_id')
            
            cursor.execute("SELECT * FROM devices WHERE websocket_id = %s", (str(id(websocket)),))
            sender_device = cursor.fetchone()
            if not sender_device: raise Exception("Устройство не найдено")
            
            cursor.execute("SELECT * FROM devices WHERE device_name = %s", (source_name,))
            source_device_info = cursor.fetchone()
            
            system_instruction = f"""Ты — ИИ-помощник {name}. РОЛЬ: Исполнитель-Аналитик.
Пользователь с устройства {source_name} изначально просил: "{original_command}".
Устройство {sender_device['device_name']} прислало системные данные (ПРОГРАММЫ И ПРОЦЕССЫ).
ПРАВИЛА:
1. Скажи пользователю на {source_name}, что задача выполнена.
2. Твои расширенные возможности как исполнителя: {CAP_EXEC}.
3. ВНИМАНИЕ: НЕ ЧИТАЙ ВЕСЬ СПИСОК ВСЛУХ! Найди нужный путь или процесс и СРАЗУ отправь финальную команду на {sender_device['device_name']} (например action_type="открытие файла" передав точный путь в action_value).
ОТВЕЧАЙ МАКСИМАЛЬНО КОРОТКО.
"""
            prompt_context = f"[ДАННЫЕ]\nПроцессы: {processes}\nПрограммы: {programs}\nВыполни задачу пользователя."

        source_id = source_device_info['id']
        cursor.execute("""
            SELECT CASE WHEN m.send_type = 'Вы' THEN 'Пользователь' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender_name, m.text
            FROM messages m LEFT JOIN devices d ON m.send_type = CAST(d.id AS CHAR) AND m.send_type NOT IN ('Вы', 'Бот')
            WHERE m.recipient_device_id = %s ORDER BY m.time ASC
        """, (source_id,))
        history_text = "\n".join([f"{msg['sender_name']}: {msg['text']}" for msg in cursor.fetchall()])
        
        cursor.execute("INSERT INTO messages (send_type, text, time, recipient_device_id) VALUES ('Бот', '', %s, %s)", (mysql_time, source_id))
        bot_message_id = cursor.lastrowid
        conn.commit()

        final_text = ""

        async for chunk in ai_instance.generate_audio_stream(
            prompt_text=prompt_context, 
            system_instruction=system_instruction,
            history_text=history_text, 
            voice_name=voice_name, 
            assistant_name=name
        ):
            if chunk["type"] == "commands":
                if chunk["commands"]: has_commands = True
                extracted_commands = chunk["commands"]
                
                for cmd in extracted_commands:
                    target_device_name = cmd.get('target_device', '').strip()
                    actions = cmd.get('actions', [])
                    if not target_device_name or not actions: continue
                    
                    cursor.execute("SELECT * FROM devices WHERE device_name = %s", (target_device_name,))
                    target_device_info = cursor.fetchone()
                    if not target_device_info:
                        cursor.execute("SELECT * FROM devices WHERE websocket_id IS NOT NULL")
                        for d in cursor.fetchall():
                            if d['device_name'].lower() in target_device_name.lower() or target_device_name.lower() in d['device_name'].lower():
                                target_device_info = d; break
                    if not target_device_info: continue

                    target_id = target_device_info['id']
                    is_source = (target_id == source_id)
                    device_spoken_text = " ".join([a.get('action_value', '') for a in actions if a.get('action_type') in ["голосовой ответ", "текстовой ответ"]])

                    target_audio_base64 = await ai_instance.generate_static_audio(device_spoken_text.strip(), voice_name, name) if (not is_source and device_spoken_text.strip()) else None

                    if target_device_info['websocket_id']:
                        target_ws = id_to_websocket.get(int(target_device_info['websocket_id']))
                        if target_ws:
                            msg_id = bot_message_id if is_source else None
                            if not is_source and device_spoken_text:
                                cursor.execute("INSERT INTO messages (send_type, text, time, recipient_device_id) VALUES (%s, %s, %s, %s)", (str(source_id), device_spoken_text.strip(), mysql_time, target_id))
                                msg_id = cursor.lastrowid; conn.commit()
                            
                            await async_send(target_ws, {
                                "type": "new_message",
                                "message_id": msg_id,
                                "user_msg_id": user_msg_id if is_source else None,
                                "sender": "Бот" if is_source else source_name,
                                "text": device_spoken_text.strip(), 
                                "actions": actions,
                                "audio_base64": target_audio_base64,
                                "source_device": source_name,
                                "original_command": original_command
                            })

            elif chunk["type"] == "bot_text":
                final_text += chunk["text"] + " "
                cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_text.strip(), bot_message_id))
                conn.commit()
                if source_device_info['websocket_id']:
                    source_ws = id_to_websocket.get(int(source_device_info['websocket_id']))
                    if source_ws:
                        await async_send(source_ws, {
                            "type": "new_message",
                            "message_id": bot_message_id,
                            "ui_msg_id": str(bot_message_id),
                            "sender": "Бот",
                            "text": chunk["text"],
                            "actions": []
                        })

            elif chunk["type"] == "audio":
                audio_chunks_count += 1
                if source_device_info['websocket_id']:
                    source_ws = id_to_websocket.get(int(source_device_info['websocket_id']))
                    if source_ws: await async_send(source_ws, {"type": "audio_chunk", "audio_base64": base64.b64encode(chunk["data"]).decode('utf-8')})

        if not final_text.strip() and audio_chunks_count == 0 and not has_commands:
            cursor.execute("DELETE FROM messages WHERE id = %s", (bot_message_id,))
            conn.commit()
            if source_device_info['websocket_id']:
                source_ws = id_to_websocket.get(int(source_device_info['websocket_id']))
                if source_ws:
                    await async_send(source_ws, {"type": "delete_message", "ui_msg_id": str(bot_message_id)})
                    await async_send(source_ws, {"type": "new_message", "message_id": None, "ui_msg_id": str(bot_message_id), "sender": "Бот", "text": "", "actions": []})
        else:
            cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (final_text.strip(), bot_message_id))
            conn.commit()
            if source_device_info['websocket_id']:
                source_ws = id_to_websocket.get(int(source_device_info['websocket_id']))
                if source_ws: await async_send(source_ws, {"type": "new_message", "message_id": bot_message_id, "ui_msg_id": str(bot_message_id), "sender": "Бот", "text": "", "actions": []})

    except Exception as e:
        logger.error(f"[ERROR] {e}", exc_info=True)
        try:
            if source_device_info and source_device_info['websocket_id']:
                source_ws = id_to_websocket.get(int(source_device_info['websocket_id']))
                if source_ws:
                    cursor.execute("DELETE FROM messages WHERE id = %s", (bot_message_id,))
                    conn.commit()
                    await async_send(source_ws, {"type": "delete_message", "ui_msg_id": str(bot_message_id)})
                    await async_send(source_ws, {"type": "new_message", "message_id": None, "ui_msg_id": str(bot_message_id), "sender": "Бот", "text": "", "actions": []})
        except: pass

# ... РЕГИСТРАЦИЯ УСТРОЙСТВА И ОБРАБОТЧИК СОКЕТОВ (оставлены без изменений, как в твоем коде)
async def handle_device_registration(websocket, data):
    conn = None
    cursor = None
    try:
        mac = data.get("MAC")
        device_name = data.get("DeviceName")
        password = data.get("Password")
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        websocket_id = id(websocket)
        
        cursor.execute("SELECT mac FROM devices WHERE device_name = %s AND mac != %s", (device_name, mac))
        if cursor.fetchone():
            await async_send(websocket, {"status": "error", "message": "Это имя устройства уже занято. Пожалуйста, выберите другое."})
            return
        
        cursor.execute("SELECT * FROM devices WHERE mac = %s", (mac,))
        device = cursor.fetchone()
        response = {"status": "success", "message": "Данные успешно обработаны!"}
        
        if device:
            updates = []
            params = []
            if device['device_name'] != device_name:
                updates.append("device_name = %s")
                params.append(device_name)
            if device['password'] != password:
                updates.append("password = %s")
                updates.append("access_list = ''")
                params.append(password)
            updates.append("websocket_id = %s")
            params.append(websocket_id)
            query = f"UPDATE devices SET {', '.join(updates)} WHERE mac = %s"
            params.append(mac)
            cursor.execute(query, params)
            
            cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
            updated_device = cursor.fetchone()
            device_id = updated_device['id'] if updated_device else None
        else:
            cursor.execute("INSERT INTO devices (mac, device_name, password, access_list, websocket_id, user_id) VALUES (%s, %s, %s, '', %s, NULL)", (mac, device_name, password, websocket_id))
            device_id = cursor.lastrowid
        
        if device_id:
            cursor.execute("""
                SELECT m.id, CASE WHEN m.send_type = 'Вы' THEN 'Вы' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender, m.text, m.time
                FROM messages m LEFT JOIN devices d ON m.send_type = CAST(d.id AS CHAR) AND m.send_type != 'Вы' AND m.send_type != 'Бот'
                WHERE m.recipient_device_id = %s ORDER BY m.time ASC
            """, (device_id,))
            messages = cursor.fetchall()
            history = [{"id": msg['id'], "sender": msg['sender'], "text": msg['text'], "time": msg['time'].strftime('%Y-%m-%d %H:%M:%S')} for msg in messages]
            response["history"] = history
        
        conn.commit()
        await async_send(websocket, response)
        
    except Exception as e:
        logger.error(f"Ошибка регистрации устройства: {e}")
        await async_send(websocket, {"status": "error", "message": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

async def websocket_handler(websocket):
    client_id = id(websocket)
    active_connections[websocket] = client_id
    id_to_websocket[client_id] = websocket
    last_ping_times[websocket] = time.time()
    
    try:
        async for message in websocket:
            try:
                decoded = base64.b64decode(message).decode('utf-8').strip().replace('\0x00', '')
                data = json.loads(decoded)
                last_ping_times[websocket] = time.time()
                
                if data.get("type") == "ping": continue
                if "DeviceName" in data: await handle_device_registration(websocket, data)
                elif "command" in data: await handle_command(websocket, data)
                elif "command_to_device" in data: await handle_target_command(websocket, data)
                elif data.get("type") == "web_client_auth": await handle_web_client_auth(websocket, data)

            except json.JSONDecodeError as e:
                pass
            except Exception as e:
                pass
    except ConnectionClosed:
        pass
    except Exception as e:
        pass
    finally:
        if websocket in active_connections: del active_connections[websocket]
        if client_id in id_to_websocket: del id_to_websocket[client_id]
        if websocket in last_ping_times: del last_ping_times[websocket]
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("UPDATE devices SET websocket_id = NULL WHERE websocket_id = %s", (client_id,))
            conn.commit()
            conn.close()
        except: pass

async def check_pings():
    while True:
        try:
            now = time.time()
            to_remove = [ws for ws, last_time in list(last_ping_times.items()) if now - last_time > PING_TIMEOUT]
            for ws in to_remove:
                last_ping_times.pop(ws, None)
                active_connections.pop(ws, None)
                try: await ws.close()
                except: pass
        except: pass
        await asyncio.sleep(10)

def run():
    server = ThreadingHTTPServer(('0.0.0.0', 25550), HTTPRequestHandler)
    logger.info("HTTPS сервер запущен на порту 25550 (Многопоточный)")
    server.serve_forever()

if __name__ == '__main__':
    run()