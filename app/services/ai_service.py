# -*- coding: utf-8 -*-
import base64
import asyncio
import logging
import traceback
import time
from datetime import datetime
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
import requests
import urllib.parse

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

    def generate_image_pollinations(self, prompt):
        encoded_prompt = urllib.parse.quote(prompt)
        url = f"https://pollinations.ai{encoded_prompt}?width=1024&height=1024&enhance=true"
        response = requests.get(url)
        if response.status_code == 200:
            return base64.b64encode(response.content).decode('utf-8')
        raise Exception("Не удалось сгенерировать изображение")

    def generate_image(self, prompt, model_type="generate"):
        models_map = {"fast": "gemini-3.1-flash-lite-image", "generate": "gemini-2.5-flash-image", "ultra": "gemini-3-pro-image"}
        model_id = models_map.get(model_type, models_map["generate"])
        
        # Микро-пауза против спама запросами
        time.sleep(1.0) 
        
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
                logger.warning(f"[Key {self.current_key_index}] Ошибка генерации: {last_error_msg}")
                
                if "429" in last_error_msg or "RESOURCE_EXHAUSTED" in last_error_msg:
                    # По умолчанию спим 5 секунд, если не найдем точное время в тексте ошибки
                    sleep_time = 5.0 
                    
                    # Пытаемся вытащить точное время ожидания (например, "Please retry in 26.749295397s.")
                    import re
                    match = re.search(r"retry in ([\d\.]+)s", last_error_msg)
                    if match:
                        try:
                            sleep_time = float(match.group(1)) + 0.5  # Добавляем 0.5с для надежности
                            logger.info(f"API требует паузу. Найдено точное время ожидания: {sleep_time} сек.")
                        except ValueError:
                            pass
                    
                    logger.info(f"Сработал лимит. Засыпаю на {sleep_time} сек. перед сменой ключа...")
                    time.sleep(sleep_time)
                
                total_keys_tried += 1
                
        raise Exception(f"AI Image Service недоступен после перебора всех ключей. Последняя ошибка: {last_error_msg}")


    async def generate_static_audio(self, text, voice_name="Aoede", assistant_name="Пятница"):
        self._rotate_key()
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"
        
        client = self._get_client()
        config = types.LiveConnectConfig(
            response_modalities=["AUDIO"], 
            speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice)))
        )
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

    # ==============================================================================
    # 1. ФУНКЦИЯ ДЛЯ ИМЯ+КОМАНДА / HTTP
    # ==============================================================================
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
                
                device_control_tool = types.Tool(
                    function_declarations=[
                        types.FunctionDeclaration(
                            name="send_device_commands",
                            description="Отправляет команды на устройства пользователя.",
                            parameters=types.Schema(
                                type=types.Type.OBJECT,
                                properties={
                                    "target_device": types.Schema(type=types.Type.STRING, description="Имя устройства"),
                                    "actions": types.Schema(
                                        type=types.Type.ARRAY,
                                        items=types.Schema(
                                            type=types.Type.OBJECT,
                                            properties={
                                                "action_type": types.Schema(type=types.Type.STRING, description=f"СТРОГО ОДИН ИЗ: {allowed_actions}"),
                                                "action_value": types.Schema(type=types.Type.STRING, description="Значение")
                                            },
                                            required=["action_type", "action_value"]
                                        )
                                    )
                                },
                                required=["target_device", "actions"]
                            )
                        )
                    ]
                )

                config = types.LiveConnectConfig(
                    response_modalities=["AUDIO"], 
                    system_instruction=types.Content(parts=[types.Part.from_text(text=system_instruction)]),
                    tools=[device_control_tool],
                    speech_config=types.SpeechConfig(
                        voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice))
                    ),
                    input_audio_transcription={},
                    output_audio_transcription={},
                    realtime_input_config=types.RealtimeInputConfig(
                        automatic_activity_detection=types.AutomaticActivityDetection(disabled=True)
                    )
                )

                logger.info(f"[CONNECT] Подключаюсь к Live API (SDK, ключ {self.current_key_index})...")
                
                cm = client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config)
                session = None
                sender_task = None
                
                try:
                    session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
                    
                    async def send_input_task():
                        try:
                            if prompt_text:
                                await session.send_realtime_input(text=prompt_text)
                            if image_bytes:
                                await session.send_realtime_input(video=types.Blob(data=image_bytes, mime_type="image/jpeg"))

                            if audio_queue:
                                while True:
                                    chunk = await audio_queue.get()
                                    if chunk is None:
                                        await session.send_realtime_input(audio_stream_end=True)
                                        break
                                    if len(chunk) > 0:
                                        await session.send_realtime_input(audio=types.Blob(data=chunk, mime_type="audio/pcm;rate=16000"))
                                        
                            elif audio_bytes:
                                pcm_data = audio_bytes[44:] if audio_bytes.startswith(b'RIFF') else audio_bytes
                                await session.send_realtime_input(activity_start=types.ActivityStart())
                                await session.send_realtime_input(audio=types.Blob(data=pcm_data, mime_type="audio/pcm;rate=16000"))
                                await session.send_realtime_input(activity_end=types.ActivityEnd())

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
                                break
                        
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
                        return 
                    raise Exception("Таймаут получения данных от Gemini (receive)")
                except StopAsyncIteration:
                    pass
                finally:
                    if sender_task: sender_task.cancel()
                    if session:
                        try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                        except: pass
                
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

    # ==============================================================================
    # 2. ФУНКЦИЯ ДЛЯ СТРИМИНГА (ТЕПЕРЬ С ПРАВИЛЬНЫМ КОНТЕКСТОМ)
    # ==============================================================================
    async def generate_audio_stream_realtime(self, prompt_text, system_instruction, allowed_actions, audio_queue, voice_name="Aoede", assistant_name="Пятница"):
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"

        total_keys_tried = 0
        while total_keys_tried < len(self.api_keys):
            self._rotate_key()
            has_yielded_data = False 
            
            try:
                client = self._get_client()
                
                device_control_tool = types.Tool(
                    function_declarations=[
                        types.FunctionDeclaration(
                            name="send_device_commands",
                            description="Отправляет команды на устройства пользователя.",
                            parameters=types.Schema(
                                type=types.Type.OBJECT,
                                properties={
                                    "target_device": types.Schema(type=types.Type.STRING, description="Имя устройства"),
                                    "actions": types.Schema(
                                        type=types.Type.ARRAY,
                                        items=types.Schema(
                                            type=types.Type.OBJECT,
                                            properties={
                                                "action_type": types.Schema(type=types.Type.STRING, description=f"СТРОГО ОДИН ИЗ: {allowed_actions}"),
                                                "action_value": types.Schema(type=types.Type.STRING, description="Значение")
                                            },
                                            required=["action_type", "action_value"]
                                        )
                                    )
                                },
                                required=["target_device", "actions"]
                            )
                        )
                    ]
                )

                config = types.LiveConnectConfig(
                    response_modalities=["AUDIO"], 
                    system_instruction=types.Content(parts=[types.Part.from_text(text=system_instruction)]),
                    tools=[device_control_tool],
                    speech_config=types.SpeechConfig(
                        voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice))
                    ),
                    input_audio_transcription={},
                    output_audio_transcription={},
                    realtime_input_config=types.RealtimeInputConfig(
                        automatic_activity_detection=types.AutomaticActivityDetection(disabled=True)
                    )
                )

                logger.info(f"[CONNECT] Подключаюсь к Live API (Streaming SDK, ключ {self.current_key_index})...")
                
                cm = client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config)
                session = None
                sender_task = None
                
                try:
                    session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
                    
                    async def send_input_task():
                        has_sent_activity_start = False
                        bytes_received = 0
                        bytes_sent = 0
                        first_chunk_time = None
                        last_chunk_time = None

                        try:
                            if prompt_text:
                                await session.send_realtime_input(text=prompt_text)

                            while True:
                                try:
                                    chunk = await asyncio.wait_for(audio_queue.get(), timeout=3.0)
                                    if chunk is None:
                                        logger.info(f"[STREAM DEBUG] Клиент прислал audio_stream_end (None). Получено: {bytes_received}, Отправлено: {bytes_sent}")
                                        if has_sent_activity_start:
                                            # ОТПРАВЛЯЕМ ТОЛЬКО ACTIVITY END! Без обнуления контекста!
                                            await session.send_realtime_input(activity_end=types.ActivityEnd())
                                        break
                                        
                                    if len(chunk) > 0:
                                        bytes_received += len(chunk)
                                        current_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                                        if first_chunk_time is None:
                                            first_chunk_time = current_time
                                        last_chunk_time = current_time

                                        if not has_sent_activity_start:
                                            await session.send_realtime_input(activity_start=types.ActivityStart())
                                            has_sent_activity_start = True

                                        await session.send_realtime_input(
                                            audio=types.Blob(data=chunk, mime_type="audio/pcm;rate=16000")
                                        )
                                        bytes_sent += len(chunk)
                                        
                                except asyncio.TimeoutError:
                                    logger.warning(f"[API STREAM DEBUG] Очередь пуста (таймаут). Получено: {bytes_received}, Отправлено: {bytes_sent}")
                                    if has_sent_activity_start:
                                        # ОТПРАВЛЯЕМ ТОЛЬКО ACTIVITY END!
                                        await session.send_realtime_input(activity_end=types.ActivityEnd())
                                    break
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
                                break
                        
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
                        return 
                    raise Exception("Таймаут получения данных от Gemini (receive)")
                except StopAsyncIteration:
                    pass
                finally:
                    if sender_task: sender_task.cancel()
                    if session:
                        try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                        except: pass
                
                return 

            except Exception as e:
                logger.error(f"[API ERROR] Ошибка на ключе {self.current_key_index}: {e}")
                if has_yielded_data: return
                total_keys_tried += 1
                if total_keys_tried < len(self.api_keys): await asyncio.sleep(1)
                else: break

        raise Exception("AI Live Service Unavailable")

ai_instance = AIService()