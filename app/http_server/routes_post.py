import json
import os
import time
import base64
import asyncio
import socket
import secrets
import bcrypt
import jwt
import hashlib
import logging
from datetime import datetime, timedelta
from app.config.settings import JWT_SECRET
from app.database.connection import get_db_connection
from app.services.ai_service import ai_instance
from app.utils.email_sender import send_email
import urllib.parse

logger = logging.getLogger("HTTP_Server")

CAP_WEB = "смена голоса (принимает СТРОГО одно из имен: Aoede/Puck/Kore/Charon), выключить микрофон (принимает любой текст), очистка истории (любой текст)"
ACT_WEB = "смена голоса, выключить микрофон, очистка истории, check_network_devices"

def notify_ws(user_id, payload):
    """Межпроцессное мгновенное уведомление WebSocket-сервера для синхронизации всех устройств пользователя."""
    if not user_id:
        return
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = json.dumps({"user_id": user_id, "payload": payload}, ensure_ascii=False).encode('utf-8')
        sock.sendto(msg, ("127.0.0.1", 8116))
        sock.close()
    except Exception as e:
        logger.error(f"Failed to notify WS server: {e}")

def do_POST(self):
    if self.path == '/api/upload':
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length <= 0:
                return self.send_json(400, {"status": "error", "message": "Файл пустой"})

            b64_filename = self.headers.get('X-Filename-B64')
            if b64_filename:
                try:
                    filename = base64.b64decode(b64_filename).decode('utf-8')
                except Exception:
                    filename = self.headers.get('X-Filename', 'file.bin')
            else:
                raw_filename = self.headers.get('X-Filename', 'file.bin')
                filename = urllib.parse.unquote(raw_filename)

            clean_filename = os.path.basename(filename)
            chat_uid = os.path.basename(self.headers.get('X-Chat-Uid', 'global'))

            upload_dir = os.path.join('/opt/friday/storage/uploads', chat_uid)
            os.makedirs(upload_dir, exist_ok=True)
            dest_path = os.path.join(upload_dir, clean_filename)

            remaining = content_length
            chunk_size = 64 * 1024
            with open(dest_path, 'wb') as f:
                while remaining > 0:
                    chunk = self.rfile.read(min(remaining, chunk_size))
                    if not chunk: break
                    f.write(chunk)
                    remaining -= len(chunk)

            logger.info(f"[Upload] Файл {clean_filename} ({content_length} байт) сохранен для чата {chat_uid}")

            download_url = f"https://friday-assistant.ru/uploads/{chat_uid}/{urllib.parse.quote(clean_filename)}"
            return self.send_json(200, {
                "status": "success",
                "filename": clean_filename,
                "url": download_url,
                "size": content_length
            })

        except Exception as e:
            logger.error(f"[Upload Error] {e}", exc_info=True)
            return self.send_json(500, {"status": "error", "message": str(e)})

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

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)

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
                message_history = message_history[-10:] 
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
5. Если пользователь просит сменить голос, выключить микрофон или очистить историю, вызови соответствующий action_type. Имена не меняй.
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
                bot_msg_id = "http_" + str(int(time.time()))
                audio_bytes = base64.b64decode(audio_base64) if audio_base64 else None
                image_bytes = base64.b64decode(screenshot_base64) if screenshot_base64 else None
                prompt_formatted = f"[ЗАПРОС С САЙТА]: {prompt}" if prompt else ""

                has_content = False
                has_stt = False

                async for chunk in ai_instance.generate_audio_stream(
                    prompt_text=prompt_formatted, system_instruction=system_instruction,
                    allowed_actions=ACT_WEB, audio_bytes=audio_bytes,
                    image_bytes=image_bytes,
                    voice_name=voice_type, assistant_name=bot_name
                ):
                    if chunk["type"] == "user_text":
                        has_stt = True
                        self._send_sse({"type": "user_transcription", "text": chunk["text"].strip()})
                    elif chunk["type"] == "bot_text":
                        has_content = True
                        self._send_sse({"type": "new_message", "message_id": bot_msg_id, "text": chunk["text"]})
                    elif chunk["type"] == "audio":
                        has_content = True
                        audio_b64 = base64.b64encode(chunk["data"]).decode('utf-8')
                        self._send_sse({"type": "audio_chunk", "audio_base64": audio_b64})
                    elif chunk["type"] == "commands":
                        has_content = True
                        for cmd in chunk.get("commands", []):
                            self._send_sse({"type": "new_message", "message_id": bot_msg_id, "actions": cmd.get("actions", [])})
                
                if audio_bytes and not has_stt:
                    self._send_sse({"type": "user_transcription", "text": "[Аудиосообщение]"})

                if not has_content:
                    self._send_sse({"type": "delete_message"})
                else:
                    self._send_sse({"type": "new_message", "message_id": bot_msg_id, "text": "", "actions": []})

            try: asyncio.run(run_ai_stream())
            except Exception as ex:
                logger.error(f"Генерация HTTP провалилась: {ex}")
                self._send_sse({"type": "delete_message"})
            finally:
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except Exception: pass
            return 

        elif self.path == '/api/get_dialogs':
            token = data.get('token')
            if not token: return self.send_json(400, {"status": "error", "message": "Токен обязателен"})
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
                user_id = payload['user_id']
                cursor.execute("SELECT id, name, created_at FROM dialogs WHERE user_id = %s ORDER BY created_at DESC", (user_id,))
                dialogs = []
                for d in cursor.fetchall():
                    dt = d['created_at'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(d['created_at'], 'strftime') else d['created_at']
                    dialogs.append({"id": d['id'], "name": d['name'], "created_at": dt})
                self.send_json(200, {"status": "success", "dialogs": dialogs})
            except Exception:
                self.send_json(401, {"status": "error", "message": "Недействительный токен"})

        elif self.path == '/api/get_history':
            token = data.get('token'); dialog_id = data.get('dialog_id')
            if not token or not dialog_id: return self.send_json(400, {"status": "error", "message": "Токен и dialog_id обязательны"})
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
                user_id = payload['user_id']
                cursor.execute("SELECT id FROM dialogs WHERE id = %s AND user_id = %s", (dialog_id, user_id))
                if not cursor.fetchone():
                    return self.send_json(403, {"status": "error", "message": "Нет доступа к этому диалогу"})
                
                # Строго по id ASC: первичный ключ гарантирует идеальный хронологический порядок создания сообщений
                cursor.execute("""
                    SELECT id, sender, device_name, text, created_at as time
                    FROM messages
                    WHERE dialog_id = %s ORDER BY id ASC
                """, (dialog_id,))
                
                history = []
                for msg in cursor.fetchall():
                    msg_time = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else msg['time']
                    history.append({
                        "id": msg['id'],
                        "sender": msg['sender'],
                        "device_name": msg.get('device_name'),
                        "text": msg['text'],
                        "time": msg_time
                    })
                
                self.send_json(200, {"status": "success", "history": history})
            except Exception:
                self.send_json(401, {"status": "error", "message": "Недействительный токен"})

        elif self.path == '/api/create_dialog':
            token = data.get('token'); name = data.get('name', 'Новый чат')
            if not token: return self.send_json(400, {"status": "error", "message": "Токен обязателен"})
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
                user_id = payload['user_id']
                cursor.execute("INSERT INTO dialogs (name, user_id) VALUES (%s, %s)", (name, user_id))
                conn.commit()
                dialog_id = cursor.lastrowid
                notify_ws(user_id, {"type": "dialog_created", "dialog_id": dialog_id, "name": name})
                self.send_json(200, {"status": "success", "dialog_id": dialog_id, "name": name})
            except Exception:
                self.send_json(401, {"status": "error", "message": "Недействительный токен"})

        elif self.path == '/api/delete_dialog':
            token = data.get('token'); dialog_id = data.get('dialog_id')
            if not token or not dialog_id: return self.send_json(400, {"status": "error", "message": "Токен и dialog_id обязательны"})
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
                user_id = payload['user_id']
                cursor.execute("DELETE FROM dialogs WHERE id = %s AND user_id = %s", (dialog_id, user_id))
                conn.commit()
                notify_ws(user_id, {"type": "dialog_deleted", "dialog_id": dialog_id})
                self.send_json(200, {"status": "success", "message": "Диалог удален"})
            except Exception:
                self.send_json(401, {"status": "error", "message": "Недействительный токен"})

        elif self.path == '/register':
            email = data.get('email')
            login = data.get('login')
            password = data.get('password')
            if not all([email, login, password]):
                return self.send_json(400, {"status": "error", "message": "Все поля обязательны"})

            # Проверяем согласие только если это явный запрос с веб-сайта с чекбоксами
            if 'terms_accepted' in data and data.get('terms_accepted') is not True:
                return self.send_json(400, {"status": "error", "message": "Необходимо принять соглашение"})
            if 'personal_data_consent' in data and data.get('personal_data_consent') is not True:
                return self.send_json(400, {"status": "error", "message": "Необходимо дать согласие на обработку данных"})

            cursor.execute("SELECT email, login FROM users WHERE email = %s OR login = %s", (email, login))
            existing = cursor.fetchall()
            if existing:
                return self.send_json(400, {"status": "error", "message": "Пользователь уже существует"})

            token = secrets.token_urlsafe(32)
            link = f"https://friday-assistant.ru/verify?token={token}"
            if send_email(email, "Подтверждение регистрации", f"Ссылка: {link}"):
                hashed_pw = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                cursor.execute(
                    "INSERT INTO users (email, login, password, SingUpToken, SingUpTokenDelTime) VALUES (%s, %s, %s, %s, NOW() + INTERVAL 1 DAY)",
                    (email, login, hashed_pw, token)
                )
                conn.commit()
                self.send_json(201, {"status": "success", "message": "Письмо отправлено"})
            else:
                self.send_json(500, {"status": "error", "message": "Ошибка отправки письма"})

        elif self.path == '/login':
            login = data.get('login'); password = data.get('password')
            cursor.execute("SELECT * FROM users WHERE email = %s OR login = %s", (login, login))
            user = cursor.fetchone()
            if not user or not bcrypt.checkpw(password.encode('utf-8'), user['password'].encode('utf-8')):
                return self.send_json(401, {"status": "error", "message": "Неверный логин или пароль"})
            if user['SingUpToken']: return self.send_json(403, {"status": "error", "message": "Аккаунт не подтвержден"})
            
            token = jwt.encode({'user_id': user['id'], 'exp': datetime.utcnow() + timedelta(days=365)}, JWT_SECRET, algorithm='HS256')
            if isinstance(token, bytes): token = token.decode('utf-8')
            self.send_json(200, {"status": "success", "message": "Вход выполнен", "user_login": user['login'], "token": token})

        elif self.path == '/login_web':
            login = data.get('login'); password = data.get('password')
            cursor.execute("SELECT * FROM users WHERE email = %s OR login = %s", (login, login))
            user = cursor.fetchone()
            if not user or not bcrypt.checkpw(password.encode('utf-8'), user['password'].encode('utf-8')): 
                return self.send_json(401, {"status": "error", "message": "Неверный логин или пароль"})
            if user['SingUpToken']: return self.send_json(403, {"status": "error", "message": "Аккаунт не подтвержден"})
            token = jwt.encode({'user_id': user['id'], 'exp': datetime.utcnow() + timedelta(days=7)}, JWT_SECRET, algorithm='HS256')
            if isinstance(token, bytes): token = token.decode('utf-8')
            self.send_json(200, {"status": "success", "message": "Вход выполнен", "user_login": user['login'], "token": token})

        elif self.path == '/logout' or self.path == '/logout_web':
            # Логаут для stateless JWT
            self.send_json(200, {"status": "success", "message": "Выход выполнен"})

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
                hashed_pw = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                cursor.execute("UPDATE users SET password = %s, RecoveryToken = NULL, RecoveryTokenDelTime = NULL WHERE id = %s", (hashed_pw, user['id']))
                conn.commit()
                self.send_json(200, {"status": "success", "message": "Пароль изменен"})
            else: self.send_json(400, {"status": "error", "message": "Неверный токен"})

        elif self.path == '/delete_message':
            msg_id = data.get('msg_id')
            token = data.get('token')
            if not msg_id: return self.send_json(400, {"status": "error", "message": "msg_id обязателен"})
            if not token: return self.send_json(401, {"status": "error", "message": "Токен обязателен"})
                
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
                user_id = payload['user_id']
            except: return self.send_json(401, {"status": "error", "message": "Invalid token"})
            
            cursor.execute("SELECT m.id, m.dialog_id FROM messages m JOIN dialogs d ON m.dialog_id = d.id WHERE m.id = %s AND d.user_id = %s", (msg_id, user_id))
            row = cursor.fetchone()
            if not row: return self.send_json(403, {"status": "error", "message": "Нет доступа к сообщению"})
            cursor.execute("DELETE FROM messages WHERE id = %s", (msg_id,))
            conn.commit()
            notify_ws(user_id, {"type": "delete_message", "dialog_id": row['dialog_id'], "message_id": msg_id, "ui_msg_id": str(msg_id)})
            self.send_json(200, {"status": "success", "message": "Сообщение удалено"})

        elif self.path == '/edit_message':
            msg_id = data.get('msg_id')
            new_text = data.get('new_text')
            token = data.get('token')
            if not msg_id or not new_text: return self.send_json(400, {"status": "error", "message": "msg_id и new_text обязательны"})
            if not token: return self.send_json(401, {"status": "error", "message": "Токен обязателен"})
                
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
                user_id = payload['user_id']
            except: return self.send_json(401, {"status": "error", "message": "Invalid token"})
            
            cursor.execute("SELECT m.id, m.dialog_id FROM messages m JOIN dialogs d ON m.dialog_id = d.id WHERE m.id = %s AND d.user_id = %s", (msg_id, user_id))
            row = cursor.fetchone()
            if not row: return self.send_json(403, {"status": "error", "message": "Нет доступа к сообщению"})
            cursor.execute("UPDATE messages SET text = %s, created_at = created_at WHERE id = %s", (new_text, msg_id))
            conn.commit()
            notify_ws(user_id, {"type": "message_edited", "dialog_id": row['dialog_id'], "message_id": msg_id, "text": new_text})
            self.send_json(200, {"status": "success", "message": "Сообщение обновлено"})

        else:
            self.send_error(404)

    except Exception as e:
        logger.error(f"Error in POST {self.path}: {e}")
        self.send_json(500, {"status": "error", "message": str(e)})
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
