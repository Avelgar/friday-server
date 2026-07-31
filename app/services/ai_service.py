# -*- coding: utf-8 -*-
import base64
import asyncio
import logging
import json
import traceback
import websockets
from google import genai
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AI_Service")

try:
    from app.config.secrets import GEMINI_KEYS
except ImportError:
    GEMINI_KEYS = []

class DeviceAction(BaseModel):
    action_type: str = Field(description="Тип действия.")
    action_value: str = Field(description="Параметр действия.")

class AIService:
    def __init__(self):
        self.api_keys = GEMINI_KEYS
        self.current_key_index = 0

    def _get_client(self):
        # SDK используем только для генерации картинок (REST работает стабильно)
        return genai.Client(http_options={"api_version": "v1beta"}, api_key=self.api_keys[self.current_key_index])

    def _rotate_key(self):
        if len(self.api_keys) <= 1: return False
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        return True

    def generate_image(self, prompt, model_type="generate"):
        models_map = {"fast": "gemini-3.1-flash-lite-image", "generate": "gemini-2.5-flash-image", "ultra": "gemini-3-pro-image"}
        model_id = models_map.get(model_type, models_map["generate"])
        total_keys_tried = 0
        last_error_msg = ""
        
        while total_keys_tried < len(self.api_keys):
            self._rotate_key()
            try:
                client = self._get_client()
                response = client.models.generate_content(model=model_id, contents=prompt)
                image_bytes = None

                if response.candidates:
                    for candidate in response.candidates:
                        if candidate.content and candidate.content.parts:
                            for part in candidate.content.parts:
                                if hasattr(part, 'inline_data') and part.inline_data and part.inline_data.data:
                                    image_bytes = part.inline_data.data; break
                        if image_bytes: break

                if not image_bytes and hasattr(response, 'inline_data') and response.inline_data: image_bytes = response.inline_data.data
                if not image_bytes:
                    try:
                        for part in response.parts:
                            if part.inline_data: image_bytes = part.inline_data.data; break
                    except: pass
                if not image_bytes: raise Exception(f"Модель {model_id} не вернула байты изображения.")

                return base64.b64encode(image_bytes).decode('utf-8')
            except Exception as e:
                last_error_msg = str(e)
                total_keys_tried += 1
        raise Exception(f"AI Image Service недоступен: {last_error_msg}")

    async def generate_static_audio(self, text, voice_name="Aoede", assistant_name="Пятница"):
        self._rotate_key()
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"
        
        ws_url = f"wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key={self.api_keys[self.current_key_index]}"
        audio_data = bytearray()
        
        try:
            async with websockets.connect(ws_url, ping_interval=None) as ws:
                setup_msg = {
                    "setup": {
                        "model": "models/gemini-3.1-flash-live-preview",
                        "generationConfig": {
                            "responseModalities": ["AUDIO"],
                            "speechConfig": {"voiceConfig": {"prebuiltVoiceConfig": {"voiceName": mapped_voice}}}
                        }
                    }
                }
                await ws.send(json.dumps(setup_msg))
                await ws.send(json.dumps({"clientContent": {"turns": [{"role": "user", "parts": [{"text": f"Произнеси: {text}"}]}], "turnComplete": True}}))

                while True:
                    try:
                        msg_raw = await asyncio.wait_for(ws.recv(), timeout=15.0)
                        response = json.loads(msg_raw)
                        if "serverContent" in response:
                            sc = response["serverContent"]
                            if "modelTurn" in sc:
                                for part in sc["modelTurn"].get("parts", []):
                                    if "inlineData" in part:
                                        audio_data.extend(base64.b64decode(part["inlineData"]["data"]))
                            if sc.get("turnComplete"):
                                break
                    except Exception:
                        break
        except Exception as e:
            logger.error(f"Static audio error: {e}")

        return base64.b64encode(audio_data).decode('utf-8') if audio_data else None

    # === ЕДИНАЯ РАБОЧАЯ ФУНКЦИЯ (ПОДДЕРЖИВАЕТ И HTTP, И СТРИМЫ, И ФАЙЛЫ) ===
    async def generate_audio_stream(self, prompt_text, system_instruction, allowed_actions, audio_bytes=None, image_bytes=None, history_text="", voice_name="Aoede", assistant_name="Пятница", audio_queue=None):
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"

        total_keys_tried = 0
        while total_keys_tried < len(self.api_keys):
            self._rotate_key()
            has_yielded_data = False 
            ws_url = f"wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key={self.api_keys[self.current_key_index]}"
            
            try:
                # RAW WEBSOCKET ПОДКЛЮЧЕНИЕ НАПРЯМУЮ К GOOGLE
                async with websockets.connect(ws_url, ping_interval=None) as ws:
                    setup_msg = {
                        "setup": {
                            "model": "models/gemini-3.1-flash-live-preview",
                            "systemInstruction": {"parts": [{"text": system_instruction}]},
                            "tools": [{"functionDeclarations": [
                                {
                                    "name": "send_device_commands",
                                    "description": "Отправляет команды на устройства пользователя.",
                                    "parameters": {
                                        "type": "OBJECT",
                                        "properties": {
                                            "target_device": {"type": "STRING", "description": "Имя устройства"},
                                            "actions": {
                                                "type": "ARRAY",
                                                "items": {
                                                    "type": "OBJECT",
                                                    "properties": {
                                                        "action_type": {"type": "STRING", "description": f"СТРОГО ОДИН ИЗ: {allowed_actions}"},
                                                        "action_value": {"type": "STRING", "description": "Значение (полная ссылка, текст или пусто)"}
                                                    },
                                                    "required": ["action_type", "action_value"]
                                                }
                                            }
                                        },
                                        "required": ["target_device", "actions"]
                                    }
                                }
                            ]}],
                            "generationConfig": {
                                "responseModalities": ["AUDIO"],
                                "speechConfig": {
                                    "voiceConfig": {
                                        "prebuiltVoiceConfig": {"voiceName": mapped_voice}
                                    }
                                }
                            }
                        }
                    }
                    
                    logger.info(f"[CONNECT] Подключаюсь к Live API (RAW WebSocket, ключ {self.current_key_index})...")
                    await ws.send(json.dumps(setup_msg))
                    
                    sender_task = None
                    
                    async def send_input_task():
                        try:
                            # 1. Текст
                            if prompt_text:
                                await ws.send(json.dumps({"clientContent": {"turns": [{"role": "user", "parts": [{"text": prompt_text}]}], "turnComplete": False}}))
                                
                            # 2. Изображение
                            if image_bytes:
                                await ws.send(json.dumps({"realtimeInput": {"video": {"mimeType": "image/jpeg", "data": base64.b64encode(image_bytes).decode('utf-8')}}}))

                            # 3. Аудио (Стриминг)
                            if audio_queue:
                                while True:
                                    try:
                                        chunk = await asyncio.wait_for(audio_queue.get(), timeout=15.0)
                                        if chunk is None:
                                            # Сообщаем об окончании ввода
                                            await ws.send(json.dumps({"clientContent": {"turnComplete": True}}))
                                            break
                                        if len(chunk) > 0:
                                            await ws.send(json.dumps({"realtimeInput": {"audio": {"mimeType": "audio/pcm;rate=16000", "data": base64.b64encode(chunk).decode('utf-8')}}}))
                                    except asyncio.TimeoutError:
                                        logger.warning("[API STREAM] Очередь пуста. Завершаем стрим.")
                                        await ws.send(json.dumps({"clientContent": {"turnComplete": True}}))
                                        break
                                        
                            # 4. Аудио (Цельный файл для HTTP или Имя+Команда)
                            elif audio_bytes:
                                pcm_data = audio_bytes[44:] if audio_bytes.startswith(b'RIFF') else audio_bytes
                                await ws.send(json.dumps({"realtimeInput": {"audio": {"mimeType": "audio/pcm;rate=16000", "data": base64.b64encode(pcm_data).decode('utf-8')}}}))
                                await ws.send(json.dumps({"clientContent": {"turnComplete": True}}))
                                
                            else:
                                # Просто отправка текста
                                await ws.send(json.dumps({"clientContent": {"turnComplete": True}}))
                                
                        except Exception as e:
                            logger.error(f"[API STREAM ERROR] {e}")

                    sender_task = asyncio.create_task(send_input_task())

                    # Ждём ответы
                    while True:
                        try:
                            msg_raw = await asyncio.wait_for(ws.recv(), timeout=45.0)
                            response = json.loads(msg_raw)

                            if "serverContent" in response:
                                sc = response["serverContent"]
                                
                                # Транскрибация
                                if "inputTranscription" in sc:
                                    yield {"type": "user_text", "text": sc["inputTranscription"].get("text", "")}
                                if "outputTranscription" in sc:
                                    has_yielded_data = True
                                    yield {"type": "bot_text", "text": sc["outputTranscription"].get("text", "")}
                                    
                                # Ответы модели (голос или текст)
                                if "modelTurn" in sc:
                                    for part in sc["modelTurn"].get("parts", []):
                                        if "inlineData" in part:
                                            has_yielded_data = True
                                            yield {"type": "audio", "data": base64.b64decode(part["inlineData"]["data"])}
                                        if "text" in part:
                                            has_yielded_data = True
                                            yield {"type": "bot_text", "text": part["text"]}
                                            
                                if sc.get("turnComplete"):
                                    logger.info("[API] Модель завершила реплику.")
                            
                            # Инструменты
                            if "toolCall" in response:
                                tool_call = response["toolCall"]
                                extracted_commands = []
                                function_responses = []

                                for fc in tool_call.get("functionCalls", []):
                                    args = fc.get("args", {})
                                    if "actions" in args:
                                        extracted_commands.append(args)
                                    
                                    function_responses.append({
                                        "id": fc["id"],
                                        "name": fc["name"],
                                        "response": {"result": "OK"}
                                    })

                                if extracted_commands:
                                    has_yielded_data = True
                                    yield {"type": "commands", "commands": extracted_commands}

                                tool_resp_msg = {"toolResponse": {"functionResponses": function_responses}}
                                await ws.send(json.dumps(tool_resp_msg))
                                
                        except asyncio.TimeoutError:
                            if has_yielded_data:
                                sender_task.cancel()
                                return 
                            raise Exception("Таймаут получения данных от Gemini (receive)")
                        except websockets.exceptions.ConnectionClosed as e:
                            logger.info(f"[API] Соединение закрыто Gemini: {e}")
                            break

                    sender_task.cancel()
                    if not has_yielded_data:
                        logger.warning("Gemini не вернул ни звука, ни текста.")
                    return 

            except Exception as e:
                logger.error(f"[API ERROR] Ошибка на ключе {self.current_key_index}: {e}")
                
                if has_yielded_data: return
                total_keys_tried += 1
                if total_keys_tried < len(self.api_keys):
                    await asyncio.sleep(1)
                else:
                    break

        raise Exception("AI Live Service Unavailable")

ai_instance = AIService()