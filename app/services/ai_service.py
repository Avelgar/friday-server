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
        import requests
        import base64
        import urllib.parse
        import random
        
        try:
            safe_prompt = urllib.parse.quote(prompt)
            url = f"https://image.pollinations.ai/prompt/{safe_prompt}"
            params = {
                "width": 512,
                "height": 512,
                "model": "flux",
                "nologo": "true",
                "enhance": "true",
                "seed": random.randint(1, 999999)
            }
            logger.info(f"Отправка GET-запроса к Pollinations AI для промпта: '{prompt}'")
            response = requests.get(url, params=params, timeout=60)
            if response.status_code == 200:
                return base64.b64encode(response.content).decode('utf-8')
            error_msg = response.text[:200] if response.text else "Нет описания ошибки"
            raise Exception(f"Pollinations API вернул код {response.status_code}. Ответ: {error_msg}")
        except requests.exceptions.Timeout:
            raise Exception("Ошибка: Время ожидания ответа от Pollinations AI истекло (более 60 секунд).")
        except Exception as e:
            raise Exception(f"Ошибка при работе с Pollinations AI: {str(e)}")

    def generate_image(self, prompt, model_type="generate"):
        models_map = {"fast": "gemini-3.1-flash-lite-image", "generate": "gemini-2.5-flash-image", "ultra": "gemini-3-pro-image"}
        model_id = models_map.get(model_type, models_map["generate"])
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
                    sleep_time = 5.0 
                    import re
                    match = re.search(r"retry in ([\d\.]+)s", last_error_msg)
                    if match:
                        try:
                            sleep_time = float(match.group(1)) + 0.5
                            logger.info(f"API требует паузу. Найдено точное время ожидания: {sleep_time} сек.")
                        except ValueError: pass
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
        
        # ЖЕСТКИЙ ПРОМПТ ДЛЯ УБИЙСТВА ОТСЕБЯТИНЫ
        sys_instr = "Ты — синтезатор речи. Твоя единственная задача: прочитать переданный текст. ЗАПРЕЩЕНО добавлять слова от себя, здороваться, прощаться или задавать вопросы вроде 'Что-то еще?'."
        
        config = types.LiveConnectConfig(
            response_modalities=["AUDIO"], 
            system_instruction=types.Content(parts=[types.Part.from_text(text=sys_instr)]),
            speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice)))
        )
        audio_data = bytearray()
        
        cm = client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config)
        session = None
        try:
            session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
            # Измененный приказ на чтение
            await session.send(input=f"Озвучь строго этот текст: {text}", end_of_turn=True)
            receive_iterator = session.receive().__aiter__()
            while True:
                response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=20.0)
                if response.server_content and response.data: 
                    audio_data.extend(response.data)
                if response.server_content and response.server_content.turn_complete:
                    break
        except: pass
        finally:
            if session:
                try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                except: pass
        return base64.b64encode(audio_data).decode('utf-8') if audio_data else None

    async def generate_static_audio_stream(self, text, voice_name="Aoede", assistant_name="Пятница"):
        """Потоковая версия TTS. Мгновенно отдает чанки аудио через yield."""
        self._rotate_key()
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"
        
        client = self._get_client()
        sys_instr = "Ты — синтезатор речи. Твоя единственная задача: прочитать переданный текст. ЗАПРЕЩЕНО добавлять слова от себя, здороваться, прощаться или задавать вопросы."
        
        config = types.LiveConnectConfig(
            response_modalities=["AUDIO"], 
            system_instruction=types.Content(parts=[types.Part.from_text(text=sys_instr)]),
            speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice)))
        )
        
        cm = client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config)
        session = None
        try:
            session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
            await session.send(input=f"Озвучь строго этот текст: {text}", end_of_turn=True)
            receive_iterator = session.receive().__aiter__()
            while True:
                response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=20.0)
                if response.server_content and response.data: 
                    yield response.data # Отдаем байты на лету!
                if response.server_content and response.server_content.turn_complete:
                    break
        except Exception as e:
            logger.error(f"[TTS STREAM ERROR]: {e}")
        finally:
            if session:
                try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                except: pass

    # ==============================================================================
    # 1. ФУНКЦИЯ ФАСАДА (Gemini 3.1 Flash Live) - Быстрое аудио и делегирование
    # ==============================================================================
    async def generate_audio_stream(self, prompt_text, system_instruction, allowed_actions, audio_bytes=None, image_bytes=None, formatted_history=None, voice_name="Aoede", assistant_name="Пятница", media_queue=None):
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

                config_kwargs = dict(
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
                
                if formatted_history:
                    config_kwargs["history_config"] = types.HistoryConfig(initial_history_in_client_content=True)

                config = types.LiveConnectConfig(**config_kwargs)

                logger.info(f"[CONNECT] Подключаюсь к Live API (SDK, ключ {self.current_key_index})...")
                
                cm = client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config)
                session = None
                sender_task = None
                
                try:
                    session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
                    
                    if formatted_history:
                        logger.info(f"[HISTORY] Отправка контекста из {len(formatted_history)} сообщений.")
                        await session.send_client_content(turns=formatted_history, turn_complete=True)
                    
                    async def send_input_task():
                        try:
                            if prompt_text:
                                await session.send_realtime_input(text=prompt_text)
                            if image_bytes:
                                await session.send_realtime_input(video=types.Blob(data=image_bytes, mime_type="image/jpeg"))

                            if media_queue:
                                while True:
                                    item = await media_queue.get()
                                    if item is None:
                                        await session.send_realtime_input(audio_stream_end=True)
                                        break
                                        
                                    if item["type"] == "audio" and len(item["data"]) > 0:
                                        await session.send_realtime_input(audio=types.Blob(data=item["data"], mime_type="audio/pcm;rate=16000"))
                                    elif item["type"] == "video" and len(item["data"]) > 0:
                                        await session.send_realtime_input(video=types.Blob(data=item["data"], mime_type="image/jpeg"))
                                        
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
                    if has_yielded_data: return 
                    raise Exception("Таймаут получения данных от Gemini (receive)")
                except StopAsyncIteration: pass
                finally:
                    if sender_task:
                        sender_task.cancel()
                        try: await sender_task
                        except asyncio.CancelledError: pass
                        except Exception: pass
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

    # ==============================================================================
    # 2. ФУНКЦИЯ ДЛЯ СТРИМИНГА С БУФЕРИЗАЦИЕЙ ПАДЕНИЙ КЛЮЧЕЙ (ФАСАД)
    # ==============================================================================
    async def generate_audio_stream_realtime(self, prompt_text, system_instruction, allowed_actions, media_queue, formatted_history=None, voice_name="Aoede", assistant_name="Пятница"):
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"

        session_audio_cache = bytearray()
        last_video_frame = None
        has_reached_stream_end = False

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

                config_kwargs = dict(
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
                
                if formatted_history:
                    config_kwargs["history_config"] = types.HistoryConfig(initial_history_in_client_content=True)

                config = types.LiveConnectConfig(**config_kwargs)

                logger.info(f"[CONNECT] Подключаюсь к Live API (Streaming SDK, ключ {self.current_key_index})...")
                
                cm = client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config)
                session = None
                sender_task = None
                
                try:
                    session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
                    
                    if formatted_history:
                        logger.info(f"[HISTORY] Отправка контекста из {len(formatted_history)} сообщений.")
                        await session.send_client_content(turns=formatted_history, turn_complete=True)
                    
                    async def send_input_task():
                        nonlocal session_audio_cache, last_video_frame, has_reached_stream_end
                        has_sent_activity_start = False

                        try:
                            if prompt_text:
                                await session.send_realtime_input(text=prompt_text)

                            if session_audio_cache:
                                await session.send_realtime_input(activity_start=types.ActivityStart())
                                has_sent_activity_start = True
                                await session.send_realtime_input(audio=types.Blob(data=bytes(session_audio_cache), mime_type="audio/pcm;rate=16000"))

                            if last_video_frame:
                                await session.send_realtime_input(video=types.Blob(data=last_video_frame, mime_type="image/jpeg"))

                            if has_reached_stream_end:
                                if has_sent_activity_start:
                                    await session.send_realtime_input(activity_end=types.ActivityEnd())
                            else:
                                while True:
                                    try:
                                        item = await asyncio.wait_for(media_queue.get(), timeout=3.0)
                                        if item is None:
                                            has_reached_stream_end = True
                                            if has_sent_activity_start:
                                                await session.send_realtime_input(activity_end=types.ActivityEnd())
                                            break
                                            
                                        if item["type"] == "audio" and len(item["data"]) > 0:
                                            chunk = item["data"]
                                            session_audio_cache.extend(chunk)

                                            if not has_sent_activity_start:
                                                await session.send_realtime_input(activity_start=types.ActivityStart())
                                                has_sent_activity_start = True

                                            await session.send_realtime_input(audio=types.Blob(data=chunk, mime_type="audio/pcm;rate=16000"))
                                            
                                        elif item["type"] == "video" and len(item["data"]) > 0:
                                            last_video_frame = item["data"]
                                            await session.send_realtime_input(video=types.Blob(data=last_video_frame, mime_type="image/jpeg"))
                                            
                                    except asyncio.TimeoutError:
                                        if has_sent_activity_start:
                                            await session.send_realtime_input(activity_end=types.ActivityEnd())
                                        break
                        except Exception as e:
                            logger.error(f"[API STREAM ERROR] {e}")

                    sender_task = asyncio.create_task(send_input_task())

                    receive_iterator = session.receive().__aiter__()
                    while True:
                        response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=20.0)
                        
                        sc = response.server_content
                        if sc:
                            if sc.input_transcription:
                                logger.info(f"[USER TRANSCRIPTION]: {sc.input_transcription.text}")
                                has_yielded_data = True
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
                    logger.warning("[API] Таймаут получения данных от Gemini (receive)")
                    if has_yielded_data: return 
                    raise Exception("Таймаут получения данных от Gemini (receive)")
                except StopAsyncIteration: pass
                finally:
                    if sender_task:
                        sender_task.cancel()
                        try: await sender_task
                        except asyncio.CancelledError: pass
                        except Exception: pass
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


    # ==============================================================================
    # 3. ТЕКСТОВОЙ ОРКЕСТРАТОР (БЫСТРЫЙ И ТЯЖЕЛЫЙ МОЗГ)
    # ==============================================================================
    async def _chat_send_with_retry(self, model_id, config, history, input_data):
        """Вспомогательный метод для отправки сообщений Мозгу с автоматической сменой ключей."""
        total_keys = len(self.api_keys)
        attempts = 0
        last_err = ""
        
        while attempts < total_keys:
            client = self._get_client()
            chat = client.aio.chats.create(model=model_id, config=config, history=history)
            try:
                response = await chat.send_message(input_data)
                return chat, response
            except Exception as e:
                last_err = str(e)
                logger.warning(f"[Key {self.current_key_index}] Ошибка Brain Chat: {last_err}")
                
                if "429" in last_err or "RESOURCE_EXHAUSTED" in last_err:
                    import re
                    match = re.search(r"retry in ([\d\.]+)s", last_err)
                    if match:
                        sleep_time = float(match.group(1)) + 0.5
                        await asyncio.sleep(sleep_time)
                        
                self._rotate_key()
                attempts += 1
                await asyncio.sleep(1)
                
        raise Exception(f"Brain Chat недоступен. Последняя ошибка: {last_err}")

    async def run_brain_orchestrator(self, prompt_text, system_instruction, allowed_actions, formatted_history, device_bridge_callback, model_id="gemini-3.5-flash-lite"):
        """
        Умный агент, который работает в цикле (Stateful ReAct).
        device_bridge_callback - асинхронная функция из handlers_cmds.py, которая ждет выполнения на клиенте.
        """
        logger.info(f"[BRAIN INIT] Запуск оркестратора на базе {model_id}...")

        safety_settings = [
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=types.HarmBlockThreshold.BLOCK_NONE),
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=types.HarmBlockThreshold.BLOCK_NONE),
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=types.HarmBlockThreshold.BLOCK_NONE),
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=types.HarmBlockThreshold.BLOCK_NONE),
        ]

        device_control_tool = types.Tool(
            function_declarations=[
                types.FunctionDeclaration(
                    name="send_device_commands",
                    description="Отправляет команды на устройства пользователя.",
                    parameters=types.Schema(
                        type=types.Type.OBJECT,
                        properties={
                            "target_device": types.Schema(type=types.Type.STRING, description="Имя целевого устройства"),
                            "actions": types.Schema(
                                type=types.Type.ARRAY,
                                items=types.Schema(
                                    type=types.Type.OBJECT,
                                    properties={
                                        "action_type": types.Schema(type=types.Type.STRING, description=f"СТРОГО ОДИН ИЗ: {allowed_actions}"),
                                        "action_value": types.Schema(type=types.Type.STRING, description="Значение (параметр) команды")
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

        config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            tools=[device_control_tool],
            safety_settings=safety_settings,
            temperature=0.2 
        )

        history = []
        if formatted_history:
            for msg in formatted_history:
                history.append(types.Content(role=msg["role"], parts=[types.Part.from_text(text=msg["parts"][0]["text"])]))

        current_input = prompt_text
        max_turns = 10 
        current_turn = 0

        while current_turn < max_turns:
            current_turn += 1
            
            chat, response = await self._chat_send_with_retry(model_id, config, history, current_input)
            history = list(chat.get_history())

            text_result = ""
            commands_to_execute = []

            if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
                for part in response.candidates[0].content.parts:
                    if part.text:
                        text_result += part.text + " "
                    if part.function_call:
                        fc = part.function_call
                        args_dict = type(fc.args).to_dict(fc.args) if hasattr(fc.args, 'to_dict') else dict(fc.args)
                        if isinstance(args_dict, dict) and "actions" in args_dict:
                            commands_to_execute.append({
                                "name": fc.name,
                                "id": getattr(fc, "id", ""),
                                "args": args_dict
                            })

            if commands_to_execute:
                logger.info(f"[BRAIN TURN {current_turn}] Мозг запросил инструменты: {len(commands_to_execute)} шт.")
                
                # Замираем и ждем, пока устройство выполнит команду и вернет ответ (например, список процессов)
                tool_results = await device_bridge_callback(commands_to_execute)
                
                function_responses = []
                for res in tool_results:
                    function_responses.append(
                        types.Part.from_function_response(
                            name=res["name"],
                            response=res["response"]
                        )
                    )
                current_input = function_responses
            else:
                final_answer = text_result.strip()
                logger.info(f"[BRAIN DONE] Цикл завершен. Ответ: {final_answer}")
                return final_answer
                
        logger.warning(f"[BRAIN] Превышен лимит шагов ({max_turns}). Принудительное завершение.")
        return text_result.strip()

    # ==============================================================================
    # 4. ТЯЖЕЛЫЙ МОЗГ (ЗРЕНИЕ И УПРАВЛЕНИЕ МЫШЬЮ)
    # ==============================================================================
    async def execute_heavy_agent(self, prompt_text, system_instruction, allowed_actions, formatted_history, device_bridge_callback, model_id="gemini-3-flash-preview"):
        """
        Тяжелый агент с компьютерным зрением (VLM). Запрашивает скриншоты и кликает.
        """
        logger.info(f"[HEAVY BRAIN] Запуск тяжелого агента на базе {model_id}...")

        safety_settings = [
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=types.HarmBlockThreshold.BLOCK_NONE),
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=types.HarmBlockThreshold.BLOCK_NONE),
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=types.HarmBlockThreshold.BLOCK_NONE),
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=types.HarmBlockThreshold.BLOCK_NONE),
        ]

        device_control_tool = types.Tool(
            function_declarations=[
                types.FunctionDeclaration(
                    name="send_device_commands",
                    description="Отправляет команды на устройства пользователя.",
                    parameters=types.Schema(
                        type=types.Type.OBJECT,
                        properties={
                            "target_device": types.Schema(type=types.Type.STRING, description="Имя целевого устройства"),
                            "actions": types.Schema(
                                type=types.Type.ARRAY,
                                items=types.Schema(
                                    type=types.Type.OBJECT,
                                    properties={
                                        "action_type": types.Schema(type=types.Type.STRING, description=f"СТРОГО ОДИН ИЗ: {allowed_actions}"),
                                        "action_value": types.Schema(type=types.Type.STRING, description="Значение (параметр) команды")
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

        config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            tools=[device_control_tool],
            safety_settings=safety_settings,
            temperature=0.0 # НУЛЕВАЯ температура для максимальной точности координат
        )

        history = []
        if formatted_history:
            for msg in formatted_history:
                history.append(types.Content(role=msg["role"], parts=[types.Part.from_text(text=msg["parts"][0]["text"])]))

        current_input = [types.Part.from_text(text=prompt_text)]
        max_turns = 15 # Даем больше шагов, так как визуальные задачи длинные (нашел->кликнул->ввел текст->отправил)
        current_turn = 0

        while current_turn < max_turns:
            current_turn += 1
            
            chat, response = await self._chat_send_with_retry(model_id, config, history, current_input)
            history = list(chat.get_history())

            text_result = ""
            commands_to_execute = []
            task_is_completed = False

            if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
                for part in response.candidates[0].content.parts:
                    if part.text:
                        text_result += part.text + " "
                    if part.function_call:
                        fc = part.function_call
                        args_dict = type(fc.args).to_dict(fc.args) if hasattr(fc.args, 'to_dict') else dict(fc.args)
                        if isinstance(args_dict, dict) and "actions" in args_dict:
                            # Проверяем, не вызвал ли ИИ "task_completed"
                            for act in args_dict.get("actions", []):
                                if act.get("action_type") == "task_completed":
                                    task_is_completed = True
                            
                            commands_to_execute.append({
                                "name": fc.name,
                                "id": getattr(fc, "id", ""),
                                "args": args_dict
                            })

            if commands_to_execute:
                logger.info(f"[HEAVY TURN {current_turn}] Мозг запросил инструменты: {len(commands_to_execute)} шт.")
                
                # Идем на C# клиент (там выполнится клик или скриншот)
                tool_results = await device_bridge_callback(commands_to_execute)
                
                # Собираем ответ для Мозга
                current_input = []
                for res in tool_results:
                    # 1. Добавляем системный ответ функции (JSON)
                    current_input.append(
                        types.Part.from_function_response(
                            name=res["name"],
                            response=res["response"]
                        )
                    )
                    
                    # 2. МУЛЬТИМОДАЛЬНАЯ МАГИЯ: Если функция вернула скриншот, прикрепляем его как картинку!
                    if res.get("attached_image_base64"):
                        logger.info("[HEAVY TURN] Прикрепляю полученный скриншот к ответу для ИИ.")
                        img_bytes = base64.b64decode(res["attached_image_base64"])
                        res_str = res.get("attached_resolution", "неизвестно")
                        
                        # Даем ИИ подсказку с разрешением, чтобы он точно высчитал X/Y
                        current_input.append(types.Part.from_text(text=f"[СИСТЕМА]: Скриншот успешно получен. Разрешение монитора: {res_str}."))
                        current_input.append(types.Part(inline_data=types.Blob(mime_type="image/jpeg", data=img_bytes)))

                if task_is_completed:
                    logger.info(f"[HEAVY DONE] Визуальная задача выполнена! Ответ: {text_result.strip()}")
                    return text_result.strip()

            else:
                final_answer = text_result.strip()
                logger.info(f"[HEAVY DONE] Цикл завершен (без вызова функций). Ответ: {final_answer}")
                return final_answer
                
        logger.warning(f"[HEAVY BRAIN] Превышен лимит шагов ({max_turns}). Принудительное завершение.")
        return text_result.strip()

ai_instance = AIService()