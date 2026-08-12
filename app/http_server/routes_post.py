import json
import time
import base64
import asyncio
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

logger = logging.getLogger("HTTP_Server")

CAP_WEB = "смена голоса (принимает СТРОГО одно из имен: Aoede/Puck/Kore/Charon), выключить микрофон (принимает любой текст), очистка истории (любой текст)"
ACT_WEB = "смена голоса, выключить микрофон, очистка истории, check_network_devices"

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
                logger.info("[HTTP] Начало генерации потока ответа...")
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
                
                if audio_bytes and not has_stt:
                    self._send_sse({"type": "user_transcription", "text": "[Аудиосообщение]"})

                if not has_content:
                    logger.info("[HTTP] ИИ ничего не ответил (пустота/таймаут).")
                    self._send_sse({"type": "delete_message"})
                else:
                    logger.info("[HTTP] Генерация потока успешно завершена.")

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
            except Exception as e:
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
                
                cursor.execute("""
                    SELECT m.id, CASE WHEN m.send_type = 'Вы' THEN 'Вы' WHEN m.send_type = 'Бот' THEN 'Бот' ELSE d.device_name END AS sender, m.text, m.created_at as time
                    FROM messages m 
                    LEFT JOIN devices d ON m.send_type COLLATE utf8mb4_general_ci = CAST(d.id AS CHAR) COLLATE utf8mb4_general_ci AND m.send_type NOT IN ('Вы', 'Бот')
                    WHERE m.dialog_id = %s ORDER BY m.created_at ASC
                """, (dialog_id,))
                
                history = []
                for msg in cursor.fetchall():
                    msg_time = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else msg['time']
                    history.append({"id": msg['id'], "sender": msg['sender'], "text": msg['text'], "time": msg_time})
                
                self.send_json(200, {"status": "success", "history": history})
            except Exception as e:
                self.send_json(401, {"status": "error", "message": "Недействительный токен"})

        elif self.path == '/api/create_dialog':
            token = data.get('token'); name = data.get('name', 'Новый чат')
            if not token: return self.send_json(400, {"status": "error", "message": "Токен обязателен"})
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
                user_id = payload['user_id']
                cursor.execute("INSERT INTO dialogs (name, user_id) VALUES (%s, %s)", (name, user_id))
                conn.commit()
                self.send_json(200, {"status": "success", "dialog_id": cursor.lastrowid, "name": name})
            except Exception as e:
                self.send_json(401, {"status": "error", "message": "Недействительный токен"})

        elif self.path == '/api/delete_dialog':
            token = data.get('token'); dialog_id = data.get('dialog_id')
            if not token or not dialog_id: return self.send_json(400, {"status": "error", "message": "Токен и dialog_id обязательны"})
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
                user_id = payload['user_id']
                cursor.execute("DELETE FROM dialogs WHERE id = %s AND user_id = %s", (dialog_id, user_id))
                conn.commit()
                self.send_json(200, {"status": "success", "message": "Диалог удален"})
            except Exception as e:
                self.send_json(401, {"status": "error", "message": "Недействительный токен"})

        elif self.path == '/api/generate_image':
            prompt = data.get('prompt')
            model_type = data.get('model_type', 'generate')
            if not prompt: return self.send_json(400, {"status": "error", "message": "Промпт не может быть пустым"})
            try:
                image_base64 = ai_instance.generate_image_pollinations(prompt)
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
                hashed_pw = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                cursor.execute("INSERT INTO users (email, login, password, SingUpToken, SingUpTokenDelTime) VALUES (%s, %s, %s, %s, NOW() + INTERVAL 1 DAY)", (email, login, hashed_pw, token))
                conn.commit()
                self.send_json(201, {"status": "success", "message": "Письмо отправлено"})
            else: self.send_json(500, {"status": "error", "message": "Ошибка отправки письма"})

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

        elif self.path == '/logout_web':
            token = data.get('token')
            if token:
                mac = f"WEB{hashlib.md5(str(token).encode()).hexdigest()[:13]}"
                cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
                dev = cursor.fetchone()
                if dev:
                    cursor.execute("UPDATE devices SET is_online = FALSE WHERE id = %s", (dev['id'],))
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
                hashed_pw = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                cursor.execute("UPDATE users SET password = %s, RecoveryToken = NULL, RecoveryTokenDelTime = NULL WHERE id = %s", (hashed_pw, user['id']))
                cursor.execute("UPDATE devices SET user_id = NULL WHERE user_id = %s", (user['id'],))
                conn.commit()
                self.send_json(200, {"status": "success", "message": "Пароль изменен"})
            else: self.send_json(400, {"status": "error", "message": "Неверный токен"})
            
        elif self.path == '/get_devices':
            mac = data.get('mac')
            cursor.execute("SELECT id, user_id FROM devices WHERE mac = %s", (mac,))
            device = cursor.fetchone()
            if not device: return self.send_json(404, {"status": "error", "message": "Устройство не найдено"})

            account_devices = []; my_devices = []; processed_macs = {mac}

            if device['user_id']:
                cursor.execute("SELECT mac, device_name, is_online FROM devices WHERE user_id = %s AND mac != %s", (device['user_id'], mac))
                for d in cursor.fetchall():
                    account_devices.append({"DeviceName": d['device_name'], "MacAddress": d['mac'], "IsOnline": bool(d['is_online']), "IsAccountDevice": True})
                    processed_macs.add(d['mac'])

            cursor.execute("SELECT d.mac, d.device_name, d.is_online FROM device_access da JOIN devices d ON da.owner_id = d.id WHERE da.guest_id = %s", (device['id'],))
            for d in cursor.fetchall():
                if d['mac'] not in processed_macs:
                    my_devices.append({"DeviceName": d['device_name'], "MacAddress": d['mac'], "IsOnline": bool(d['is_online']), "IsAccountDevice": False})
                    processed_macs.add(d['mac'])

            cursor.execute("SELECT d.mac, d.device_name, d.is_online FROM device_access da JOIN devices d ON da.guest_id = d.id WHERE da.owner_id = %s", (device['id'],))
            for d in cursor.fetchall():
                if d['mac'] not in processed_macs:
                    my_devices.append({"DeviceName": d['device_name'], "MacAddress": d['mac'], "IsOnline": bool(d['is_online']), "IsAccountDevice": False})
                    processed_macs.add(d['mac'])

            self.send_json(200, {"status": "success", "account_devices": account_devices, "my_devices": my_devices})

        elif self.path == '/connect_device':
            req_mac = data.get('MAC'); dev_name = data.get('DeviceName'); pwd = data.get('Password')
            cursor.execute("SELECT id, mac, password FROM devices WHERE device_name = %s", (dev_name,))
            target = cursor.fetchone()
            cursor.execute("SELECT id FROM devices WHERE mac = %s", (req_mac,))
            requester = cursor.fetchone()
            
            if not target: return self.send_json(404, {"status": "error", "message": "Устройство не найдено"})
            if not bcrypt.checkpw(pwd.encode('utf-8'), target['password'].encode('utf-8')): return self.send_json(401, {"status": "error", "message": "Неверный пароль"})
            if req_mac == target['mac']: return self.send_json(400, {"status": "error", "message": "Само к себе"})
            if not requester: return self.send_json(404, {"status": "error", "message": "Инициатор не найден"})
            
            cursor.execute("INSERT IGNORE INTO device_access (owner_id, guest_id) VALUES (%s, %s), (%s, %s)", (target['id'], requester['id'], requester['id'], target['id']))
            conn.commit()
            self.send_json(200, {"status": "success", "message": "Подключено", "target_mac": target['mac'], "target_device_name": dev_name})

        elif self.path == '/disconnect_device':
            req_mac = data.get('requester_mac'); target_mac = data.get('target_mac')
            cursor.execute("SELECT id, mac FROM devices WHERE mac IN (%s, %s)", (req_mac, target_mac))
            devs = {r['mac']: r['id'] for r in cursor.fetchall()}
            if len(devs) != 2: return self.send_json(404, {"status": "error", "message": "Устройства не найдены"})
            req_id = devs[req_mac]; tar_id = devs[target_mac]
            cursor.execute("DELETE FROM device_access WHERE (owner_id=%s AND guest_id=%s) OR (owner_id=%s AND guest_id=%s)", (req_id, tar_id, tar_id, req_id))
            conn.commit()
            self.send_json(200, {"status": "success", "message": "Отключено"})

        elif self.path == '/clear_history':
            token = data.get('token')
            dialog_id = data.get('dialog_id')
            
            if dialog_id and token:
                return self.send_json(403, {"status": "error", "message": "Очистка истории для аккаунта отключена."})
            else:
                if token: 
                    mac = f"WEB{hashlib.md5(str(token).encode()).hexdigest()[:13]}"
                else:
                    mac = data.get('mac')
                cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
                dev = cursor.fetchone()
                if dev:
                    cursor.execute("DELETE FROM messages WHERE recipient_device_id = %s", (dev['id'],))
                    conn.commit()
                    self.send_json(200, {"status": "success", "message": "История очищена"})
                else: 
                    self.send_json(404, {"status": "error", "message": "Устройство не найдено"})
            
        # === ФИКС 404 ДЛЯ УДАЛЕНИЯ И РЕДАКТИРОВАНИЯ ===
        elif self.path == '/delete_message':
            msg_id = data.get('msg_id')
            token = data.get('token')
            mac = data.get('mac')
            
            if not msg_id:
                return self.send_json(400, {"status": "error", "message": "msg_id обязателен"})
                
            if token:
                try:
                    payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
                    user_id = payload['user_id']
                except:
                    return self.send_json(401, {"status": "error", "message": "Invalid token"})
                
                # Ищем сообщение по ВЛАДЕЛЬЦУ диалога, а не по устройству
                cursor.execute("""
                    SELECT m.id FROM messages m 
                    JOIN dialogs d ON m.dialog_id = d.id 
                    WHERE m.id = %s AND d.user_id = %s
                """, (msg_id, user_id))
                if not cursor.fetchone():
                    return self.send_json(403, {"status": "error", "message": "Нет доступа к сообщению"})
                    
                cursor.execute("DELETE FROM messages WHERE id = %s", (msg_id,))
            else:
                # Гостевой режим (осталось как было, по MAC-адресу)
                if not mac: return self.send_json(400, {"status": "error", "message": "mac обязателен для гостей"})
                cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
                dev = cursor.fetchone()
                if not dev: return self.send_json(404, {"status": "error", "message": "Устройство не найдено"})
                cursor.execute("DELETE FROM messages WHERE id = %s AND recipient_device_id = %s", (msg_id, dev['id']))
                if cursor.rowcount == 0: return self.send_json(404, {"status": "error", "message": "Сообщение не найдено в БД"})
                
            conn.commit()
            self.send_json(200, {"status": "success", "message": "Сообщение удалено"})

        elif self.path == '/edit_message':
            msg_id = data.get('msg_id')
            new_text = data.get('new_text')
            token = data.get('token')
            mac = data.get('mac')
            
            if not msg_id or not new_text:
                return self.send_json(400, {"status": "error", "message": "msg_id и new_text обязательны"})
                
            if token:
                try:
                    payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
                    user_id = payload['user_id']
                except:
                    return self.send_json(401, {"status": "error", "message": "Invalid token"})
                    
                # Ищем сообщение по ВЛАДЕЛЬЦУ диалога
                cursor.execute("""
                    SELECT m.id FROM messages m 
                    JOIN dialogs d ON m.dialog_id = d.id 
                    WHERE m.id = %s AND d.user_id = %s
                """, (msg_id, user_id))
                if not cursor.fetchone():
                    return self.send_json(403, {"status": "error", "message": "Нет доступа к сообщению"})
                    
                cursor.execute("UPDATE messages SET text = %s WHERE id = %s", (new_text, msg_id))
            else:
                # Гостевой режим
                if not mac: return self.send_json(400, {"status": "error", "message": "mac обязателен для гостей"})
                cursor.execute("SELECT id FROM devices WHERE mac = %s", (mac,))
                dev = cursor.fetchone()
                if not dev: return self.send_json(404, {"status": "error", "message": "Устройство не найдено"})
                cursor.execute("UPDATE messages SET text = %s WHERE id = %s AND recipient_device_id = %s", (new_text, msg_id, dev['id']))
                if cursor.rowcount == 0: return self.send_json(404, {"status": "error", "message": "Сообщение не найдено в БД"})
                
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