# -*- coding: utf-8 -*-
import base64
import asyncio
import logging
import traceback
from google import genai
from google.genai import types
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

def send_device_commands(target_device: str, actions: list[DeviceAction]):
    pass

class AIService:
    def __init__(self):
        self.api_keys = GEMINI_KEYS
        self.current_key_index = 0

    def _get_client(self):
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
        
        client = self._get_client()
        config = {
            "response_modalities": ["AUDIO"], 
            "speech_config": {"voice_config": {"prebuilt_voice_config": {"voice_name": mapped_voice}}}
        }
        audio_data = bytearray()
        
        cm = client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config)
        session = None
        try:
            session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
            await session.send(input=f"Произнеси: {text}", end_of_turn=True)
            receive_iterator = session.receive().__aiter__()
            while True:
                response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=20.0)
                if response.server_content and response.data: audio_data.extend(response.data)
        except: pass
        finally:
            if session:
                try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                except: pass
        return base64.b64encode(audio_data).decode('utf-8') if audio_data else None

    async def generate_audio_stream(self, prompt_text, system_instruction, allowed_actions, audio_bytes=None, image_bytes=None, history_text="", voice_name="Aoede", assistant_name="Пятница", audio_queue=None):
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"

        total_keys_tried = 0
        while total_keys_tried < len(self.api_keys):
            self._rotate_key()
            has_yielded_data = False 
            
            try:
                client = self._get_client()
                
                device_control_tool = {
                    "function_declarations": [
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
                    ]
                }

                config = {
                    "system_instruction": {"parts": [{"text": system_instruction}]},
                    "tools": [device_control_tool],
                    "response_modalities": ["AUDIO"],
                    "input_audio_transcription": {},
                    "output_audio_transcription": {},
                    "speech_config": {
                        "voice_config": {
                            "prebuilt_voice_config": {
                                "voice_name": mapped_voice
                            }
                        }
                    }
                }

                logger.info(f"[CONNECT] Подключаюсь к Live API (SDK, ключ {self.current_key_index})...")
                
                cm = client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config)
                session = None
                sender_task = None
                
                try:
                    session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
                    
                    async def send_input_task():
                        try:
                            # 1. Текст
                            if prompt_text:
                                await session.send(input=prompt_text, end_of_turn=False)
                            
                            # 2. Картинка
                            if image_bytes:
                                await session.send(input={"mime_type": "image/jpeg", "data": image_bytes}, end_of_turn=False)

                            # 3. Аудио (Стриминг: Сайт с Аккаунтом / Разговорный с ПК)
                            if audio_queue:
                                while True:
                                    try:
                                        chunk = await asyncio.wait_for(audio_queue.get(), timeout=15.0)
                                        if chunk is None:
                                            # Конец стрима -> говорим Gemini, что очередь пуста
                                            await session.send(input="", end_of_turn=True)
                                            break
                                        if len(chunk) > 0:
                                            # ИСПРАВЛЕНИЕ: SDK требует сырые байты, а не Blob (дока врёт).
                                            await session.send(input=chunk, end_of_turn=False)
                                    except asyncio.TimeoutError:
                                        logger.warning("[API STREAM] Очередь пуста. Завершаем стрим.")
                                        await session.send(input="", end_of_turn=True)
                                        break
                                        
                            # 4. Аудио (Цельный файл: Гость сайта HTTP / Имя+Команда на ПК)
                            elif audio_bytes:
                                pcm_data = audio_bytes[44:] if audio_bytes.startswith(b'RIFF') else audio_bytes
                                # ИСПРАВЛЕНИЕ: Передаем сырые байты напрямую, без оберток Blob. SDK сам сделает из них pcm!
                                await session.send(input=pcm_data, end_of_turn=True)
                            
                            # 5. Только текст/картинка (Аудио нет)
                            else:
                                await session.send(input="", end_of_turn=True)
                                
                        except Exception as e:
                            logger.error(f"[API STREAM ERROR] {e}")

                    sender_task = asyncio.create_task(send_input_task())

                    receive_iterator = session.receive().__aiter__()
                    while True:
                        response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=35.0)
                        
                        sc = response.server_content
                        if sc:
                            if sc.input_transcription:
                                yield {"type": "user_text", "text": sc.input_transcription.text}
                            if sc.output_transcription:
                                has_yielded_data = True
                                yield {"type": "bot_text", "text": sc.output_transcription.text}
                            if sc.model_turn:
                                for part in sc.model_turn.parts:
                                    if part.inline_data:
                                        has_yielded_data = True
                                        yield {"type": "audio", "data": part.inline_data.data}
                            if sc.turn_complete:
                                logger.info("[API] Модель завершила реплику.")
                        
                        if response.tool_call:
                            extracted_commands = []
                            function_responses = []
                            for fc in response.tool_call.function_calls:
                                args_dict = type(fc.args).to_dict(fc.args) if hasattr(fc.args, 'to_dict') else dict(fc.args)
                                if isinstance(args_dict, dict) and "actions" in args_dict:
                                    extracted_commands.append(args_dict)
                                function_responses.append(types.FunctionResponse(name=fc.name, id=fc.id, response={"result": "OK"}))
                            
                            if extracted_commands:
                                has_yielded_data = True
                                yield {"type": "commands", "commands": extracted_commands}
                            
                            await session.send_tool_response(function_responses=function_responses)
                            
                except (asyncio.TimeoutError, TimeoutError):
                    if has_yielded_data:
                        logger.info("[API] Таймаут после отправки данных. Считаем ответ ИИ завершенным.")
                        return 
                    raise Exception("Таймаут получения данных от Gemini (receive)")
                except StopAsyncIteration:
                    pass
                finally:
                    if sender_task: sender_task.cancel()
                    if session:
                        try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                        except: pass
                
                if not has_yielded_data:
                    raise Exception("Gemini не вернул ни звука, ни текста.")
                
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