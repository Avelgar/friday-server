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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("HTTP_Server")

CAP_WEB = "смена голоса (принимает СТРОГО одно из имен: Aoede/Puck/Kore/Charon), очистка истории (любой текст)"

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
        except Exception:
            pass
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
            time.sleep(3600)

threading.Thread(target=clean_expired_tokens, daemon=True).start()

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    pass

class HTTPRequestHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    
    def send_json(self, status_code, data):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_sse(self, data):
        try:
            msg = f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode('utf-8')
            chunk_header = f"{len(msg):X}\r\n".encode('utf-8')
            self.wfile.write(chunk_header)
            self.wfile.write(msg)
            self.wfile.write(b"\r\n")
            self.wfile.flush()
        except Exception as e:
            logger.error(f"Ошибка при отправке SSE чанка: {e}")

    def serve_file(self, filename, content_type, download_name=None):
        try:
            file_path = os.path.join('/opt/friday', filename)
            with open(file_path, 'rb') as f:
                content = f.read()
                self.send_response(200)
                self.send_header('Content-type', content_type)
                self.send_header('Content-Length', str(len(content)))
                if download_name: self.send_header('Content-Disposition', f'attachment; filename="{download_name}"')
                self.end_headers()
                self.wfile.write(content)
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
                bot_name = "Пятница"
                screenshot_base64 = data.get('screenshot')
                message_history = data.get('message_history', [])

                if not prompt and not audio_base64 and not screenshot_base64:
                    return self.send_json(400, {"status": "error", "message": "Пустой запрос"})

                history_text = ""
                if message_history:
                    history_text = "\n\nИСТОРИЯ ДИАЛОГА (КОНТЕКСТ):\n"
                    for msg in message_history:
                        role = "Пользователь" if msg.get('role') == 'user' else "Ассистент"
                        content = msg.get('content', '')[:500]
                        history_text += f"{role}: {content}\n"

                system_instruction = f"""Ты — ИИ-помощник {bot_name}. Твой собеседник находится на ВЕБ-САЙТЕ как неавторизованный гость.
ПРАВИЛА:
1. Ты общаешься ТОЛЬКО ГОЛОСОМ. Говори естественно и живо.
2. Твой голос АВТОМАТИЧЕСКИ транслируется пользователю на сайт. Не используй "голосовой ответ" как action_type.
3. Гости сайта НЕ ИМЕЮТ доступа к устройствам (ПК или телефону). Ты не можешь открывать им программы или искать устройства в сети.
4. Доступные локальные команды управления сайтом: {CAP_WEB}. 
5. Если пользователь просит сменить голос или очистить историю, вызови соответствующий action_type. Имена не меняй.
{history_text}
"""
                
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream; charset=utf-8')
                self.send_header('Cache-Control', 'no-cache')
                self.send_header('Connection', 'keep-alive')
                self.send_header('Transfer-Encoding', 'chunked') 
                self.send_header('X-Accel-Buffering', 'no')      
                self.end_headers()

                async def run_ai_stream():
                    logger.info("[HTTP] Начало генерации потока ответа...")
                    bot_msg_id = "http_" + str(int(time.time()))
                    audio_bytes = base64.b64decode(audio_base64) if audio_base64 else None
                    image_bytes = base64.b64decode(screenshot_base64) if screenshot_base64 else None
                    prompt_formatted = f"[ЗАПРОС С САЙТА]: {prompt}" if prompt else ""

                    has_content = False

                    async for chunk in ai_instance.generate_audio_stream(
                        prompt_text=prompt_formatted,
                        system_instruction=system_instruction,
                        allowed_actions="смена голоса, очистка истории",
                        audio_bytes=audio_bytes,
                        image_bytes=image_bytes,
                        voice_name=voice_type,
                        assistant_name=bot_name
                    ):
                        if chunk["type"] == "user_text":
                            logger.info(f"[HTTP ТРАНСКРИБАЦИЯ ЮЗЕРА]: {chunk['text'].strip()}")
                            self._send_sse({"type": "user_transcription", "text": chunk["text"].strip()})
                        
                        elif chunk["type"] == "bot_text":
                            has_content = True
                            logger.info(f"[HTTP ЧАНК ТЕКСТА]: {chunk['text'].strip()}")
                            self._send_sse({"type": "new_message", "message_id": bot_msg_id, "text": chunk["text"]})
                        
                        elif chunk["type"] == "audio":
                            has_content = True
                            audio_b64 = base64.b64encode(chunk["data"]).decode('utf-8')
                            self._send_sse({"type": "audio_chunk", "audio_base64": audio_b64})
                        
                        elif chunk["type"] == "commands":
                            has_content = True
                            logger.info(f"[HTTP ЧАНК КОМАНД]: {chunk.get('commands')}")
                            for cmd in chunk.get("commands", []):
                                self._send_sse({"type": "new_message", "message_id": bot_msg_id, "actions": cmd.get("actions", [])})
                    
                    if not has_content:
                        logger.info("[HTTP] ИИ ничего не ответил (пустота/таймаут).")
                        self._send_sse({"type": "delete_message"})
                    else:
                        logger.info("[HTTP] Генерация потока успешно завершена.")

                try:
                    asyncio.run(run_ai_stream())
                except Exception as ex:
                    logger.error(f"Генерация HTTP провалилась: {ex}")
                    self._send_sse({"type": "delete_message"})
                finally:
                    try:
                        self.wfile.write(b"0\r\n\r\n")
                        self.wfile.flush()
                    except Exception:
                        pass
                return 

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
            
            # === ВОССТАНОВЛЕННЫЕ РОУТЫ ДЛЯ API САЙТА ===
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

def run():
    server = ThreadingHTTPServer(('0.0.0.0', 25550), HTTPRequestHandler)
    logger.info("HTTPS сервер запущен на порту 25550 (Многопоточный)")
    server.serve_forever()

if __name__ == '__main__':
    run()