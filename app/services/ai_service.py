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
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AI_Service")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

try:
    from app.config.secrets import GEMINI_KEYS
except ImportError:
    GEMINI_KEYS = []

class DeviceAction(BaseModel):
    action_type: str = Field(description="Тип действия.")
    action_value: str = Field(description="Параметр действия.")

def send_device_commands(target_device: str, actions: list[DeviceAction]):
    pass

class GeminiKeyPool:
    """Менеджер пула API-ключей Gemini с отслеживанием лимитов и ротацией."""
    def __init__(self, keys: list[str]):
        self.keys = keys or []
        self.exhausted_keys = {}  # key_index -> unban_timestamp

    def mark_exhausted(self, key_index: int, duration_seconds: float = 3600 * 12):
        now = time.time()
        self.exhausted_keys[key_index] = now + duration_seconds
        logger.warning(f"🚫 [Key Pool] Ключ #{key_index} помечен как ИСЧЕРПАННЫЙ на {duration_seconds/3600:.1f}ч.")

    def is_usable(self, key_index: int) -> bool:
        if not self.keys:
            return True
        if key_index not in self.exhausted_keys:
            return True
        if time.time() > self.exhausted_keys[key_index]:
            del self.exhausted_keys[key_index]
            return True
        return False

    def get_available_key(self, preferred_index: int = None) -> int:
        total = len(self.keys)
        if total <= 1:
            return 0
        
        # Если переданный ключ валиден и не в бане — остаемся на нем
        if preferred_index is not None and 0 <= preferred_index < total:
            if self.is_usable(preferred_index):
                return preferred_index

        # Ищем следующий свободный ключ
        start = (preferred_index + 1) % total if preferred_index is not None else 0
        for i in range(total):
            idx = (start + i) % total
            if self.is_usable(idx):
                return idx

        # Если все ключи временно заблокированы — разблокируем тот, у кого бан кончится быстрее
        if self.exhausted_keys:
            earliest = min(self.exhausted_keys.keys(), key=lambda k: self.exhausted_keys[k])
            logger.warning(f"⚠️ [Key Pool] Все ключи исчерпаны! Принудительно разблокируем ключ #{earliest}")
            del self.exhausted_keys[earliest]
            return earliest

        return 0

class AIService:
    def __init__(self):
        self.api_keys = GEMINI_KEYS
        self.current_key_index = 0
        self.antigravity_sessions = {}
        self.key_pool = GeminiKeyPool(self.api_keys)

    def _get_client(self, key_index=None):
        idx = key_index if key_index is not None else self.current_key_index
        return genai.Client(http_options={"api_version": "v1beta"}, api_key=self.api_keys[idx])

    def _get_live_client(self):
        live_key = self.api_keys[0] if self.api_keys else None
        return genai.Client(http_options={"api_version": "v1beta"}, api_key=live_key)

    def _rotate_key(self):
        if len(self.api_keys) <= 1: return False
        self.current_key_index = self.key_pool.get_available_key(self.current_key_index)
        return True

    # ==============================================================================
    # 1. СИНТЕЗ РЕЧИ (TTS)
    # ==============================================================================
    async def generate_static_audio(self, text, voice_name="Aoede", assistant_name="Пятница"):
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"
        
        client = self._get_live_client()
        sys_instr = "Ты — синтезатор речи. Твоя единственная задача: прочитать переданный текст без отсебятины."
        
        config = types.LiveConnectConfig(
            response_modalities=["AUDIO"], 
            system_instruction=types.Content(parts=[types.Part.from_text(text=sys_instr)]),
            speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice)))
        )
        audio_data = bytearray()
        cm = client.aio.live.connect(model="models/gemini-3.8-live", config=config)
        session = None
        try:
            session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
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
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"
        
        client = self._get_live_client()
        sys_instr = "Ты — синтезатор речи. Твоя единственная задача: прочитать переданный текст без отсебятины."
        
        config = types.LiveConnectConfig(
            response_modalities=["AUDIO"], 
            system_instruction=types.Content(parts=[types.Part.from_text(text=sys_instr)]),
            speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice)))
        )
        cm = client.aio.live.connect(model="models/gemini-3.8-live", config=config)
        session = None
        try:
            session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
            await session.send(input=f"Озвучь строго этот текст: {text}", end_of_turn=True)
            receive_iterator = session.receive().__aiter__()
            while True:
                response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=20.0)
                if response.server_content and response.data: 
                    yield response.data
                if response.server_content and response.server_content.turn_complete:
                    break
        except Exception as e:
            logger.error(f"[TTS STREAM ERROR]: {e}")
        finally:
            if session:
                try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                except: pass

    # ==============================================================================
    # 2. ФАСАД (Gemini 3.1 Flash Live)
    # ==============================================================================
    async def generate_audio_stream(self, prompt_text, system_instruction, allowed_actions, audio_bytes=None, image_bytes=None, formatted_history=None, voice_name="Aoede", assistant_name="Пятница", media_queue=None):
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"
        has_yielded_data = False 
        
        try:
            client = self._get_live_client()
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
            cm = client.aio.live.connect(model="models/gemini-3.8-live", config=config)
            session = None
            sender_task = None
            
            try:
                session = await asyncio.wait_for(cm.__aenter__(), timeout=12.0)
                if formatted_history:
                    try: await session.send_client_content(turns=formatted_history[-10:], turn_complete=True)
                    except: pass
                
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
                    response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=30.0)
                    if response.tool_call:
                        extracted_commands = []
                        function_responses = []
                        for fc in response.tool_call.function_calls:
                            args_dict = type(fc.args).to_dict(fc.args) if hasattr(fc.args, 'to_dict') else dict(fc.args)
                            if isinstance(args_dict, dict) and "actions" in args_dict:
                                extracted_commands.append(args_dict)
                            function_responses.append(types.FunctionResponse(
                                name=fc.name, 
                                id=fc.id, 
                                response={"status": "ok", "retryable": False, "message": "Success"}
                            ))
                        
                        if extracted_commands:
                            has_yielded_data = True
                            yield {"type": "commands", "commands": extracted_commands}
                        await session.send_tool_response(function_responses=function_responses)
                        
                    sc = response.server_content
                    if sc:
                        if sc.input_transcription:
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
                            break
                        
            except (asyncio.TimeoutError, TimeoutError):
                if has_yielded_data: return 
            except StopAsyncIteration: pass
            finally:
                if sender_task:
                    sender_task.cancel()
                    try: await sender_task
                    except: pass
                if session:
                    try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                    except: pass
            return 
        except Exception as e:
            logger.error(f"[API ERROR] Ошибка Live API: {e}")
            if has_yielded_data: return
            raise Exception("AI Live Service Unavailable")

    async def generate_audio_stream_realtime(self, prompt_text, system_instruction, allowed_actions, media_queue, formatted_history=None, voice_name="Aoede", assistant_name="Пятница"):
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"
        system_instruction += "\n\nCRITICAL RULE: You MUST speak a short voice confirmation (like 'Секунду', 'Готово') BEFORE calling any tools. Never call a tool silently!"
        session_audio_cache = bytearray()
        last_video_frame = None
        has_reached_stream_end = False
        has_yielded_data = False 
        
        try:
            client = self._get_live_client()
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
            cm = client.aio.live.connect(model="models/gemini-3.8-live", config=config)
            session = None
            sender_task = None
            
            try:
                session = await asyncio.wait_for(cm.__aenter__(), timeout=12.0)
                if formatted_history:
                    try: await session.send_client_content(turns=formatted_history[-10:], turn_complete=True)
                    except: pass
                
                async def send_input_task():
                    nonlocal session_audio_cache, last_video_frame, has_reached_stream_end
                    has_sent_activity_start = False
                    try:
                        if prompt_text: await session.send_realtime_input(text=prompt_text)
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
                                    item = await asyncio.wait_for(media_queue.get(), timeout=2.5)
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
                    response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=30.0)
                    if response.tool_call:
                        extracted_commands = []
                        function_responses = []
                        for fc in response.tool_call.function_calls:
                            args_dict = type(fc.args).to_dict(fc.args) if hasattr(fc.args, 'to_dict') else dict(fc.args)
                            if isinstance(args_dict, dict) and "actions" in args_dict:
                                extracted_commands.append(args_dict)
                            function_responses.append(types.FunctionResponse(
                                name=fc.name, id=fc.id, response={"status": "ok", "retryable": False, "message": "Success"}
                            ))
                        if extracted_commands:
                            has_yielded_data = True
                            yield {"type": "commands", "commands": extracted_commands}
                        await session.send_tool_response(function_responses=function_responses)
                        
                    sc = response.server_content
                    if sc:
                        if sc.input_transcription:
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
                        if sc.turn_complete: break
            except: pass
            finally:
                if sender_task: sender_task.cancel()
                if session:
                    try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                    except: pass
            return
        except Exception as e:
            if has_yielded_data: return
            raise Exception("AI Live Service Unavailable")

    # ==============================================================================
    # 3. ТЕКСТОВЫЙ ОРКЕСТРАТОР (Fast Brain)
    # ==============================================================================
    async def _chat_send_with_retry(self, model_id, config, history, input_data):
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
                logger.warning(f"[Key {self.current_key_index}] Ошибка Brain Chat: {last_err[:120]}")
                self._rotate_key()
                attempts += 1
                
        raise Exception(f"Brain Chat недоступен после перебора {total_keys} ключей. Ошибка: {last_err}")

    async def run_brain_orchestrator(self, prompt_text, system_instruction, allowed_actions, formatted_history, device_bridge_callback, model_id="gemini-3.8-flash"):
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
                                        "action_value": types.Schema(type=types.Type.STRING, description="Значение команды")
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
            temperature=0.2
        )

        history = [types.Content(role=msg["role"], parts=[types.Part.from_text(text=msg["parts"][0]["text"])]) for msg in (formatted_history or [])]
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
                    if part.text: text_result += part.text + " "
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
                return text_result.strip()
                
        return text_result.strip()

    # ==============================================================================
    # 4. АВТОНОМНЫЙ АГЕНТ ANTIGRAVITY (С ПУЛОМ КЛЮЧЕЙ И SESSION HANDOFF)
    # ==============================================================================
    async def run_antigravity_agent(
        self, 
        prompt_text: str, 
        status_callback=None, 
        target_files: list = None, 
        input_files: dict = None,
        previous_interaction_id: str = None,
        environment_id: str = None,
        assigned_key: int = None,
        chat_uid: str = None,
        recent_history: list = None
    ):
        total_keys = len(self.api_keys)
        is_new_session = (previous_interaction_id is None)

        if assigned_key is None:
            if is_new_session:
                assigned_key = self.key_pool.get_available_key()
            else:
                assigned_key = self.antigravity_sessions.get(previous_interaction_id, self.current_key_index)
                if not self.key_pool.is_usable(assigned_key):
                    assigned_key = self.key_pool.get_available_key(assigned_key)
        else:
            if not self.key_pool.is_usable(assigned_key):
                logger.info(f"[Antigravity Engine] Ключ #{assigned_key} исчерпан. Ротируем на свободный.")
                assigned_key = self.key_pool.get_available_key(assigned_key)

        logger.info(f"[Antigravity Engine] Запуск таски на ключе #{assigned_key} (new={is_new_session})")

        agent_system_instruction = """Ты — автономный Senior разработчик.
Все файлы проекта (код, верстку, скрипты, презы, 3D-модели, Word-документы) ты создаешь, изменяешь и тестируешь прямо в своем окружении Linux.

Твое взаимодействие с компьютером пользователя строится через теги:
1. [GET_TREE] — запросить дерево проекта у пользователя.
2. [GET_FILES: путь1, путь2] — запросить содержимое файлов с компьютера пользователя.
3. [SYNC: относительный_путь] — синхронизировать созданный или измененный тобой файл на компьютер пользователя. 
   Сначала создай/отредактируй файл в своей файловой системе, протестируй его, а затем укажи [SYNC: путь].
4. [RUN_COMMAND: команда] — выполнить системную команду на компьютере пользователя (Windows). Используй `python`, а не `python3`.
5. [ASK_USER: Текст вопроса | Вариант 1 | Вариант 2 | Вариант 3] — уточнить параметры задачи или спросить мнение пользователя.
6. [DELETE: относительный_путь] — удалить файл или папку на компьютере пользователя (без использования терминала). Клиент безопасно удалит его через файловую систему.

ПРАВИЛА:
- На простые вопросы отвечай текстом без тегов.
- ЗАПРЕЩЕНО выводить полные тексты файлов в чат! Создавай файлы в файловой системе и используй [SYNC: путь].
- Для удаления файлов на компьютере пользователя строго используй [DELETE: путь], не пытайся вызывать del или rm через RUN_COMMAND.
"""

        max_attempts = max(3, min(total_keys, 10)) if total_keys > 0 else 3
        attempt = 0

        while attempt < max_attempts:
            client = self._get_client(key_index=assigned_key)

            try:
                kwargs = {
                    "agent": "antigravity-preview-09-2026",
                    "background": True,
                }

                current_input_prompt = prompt_text

                # Если сессия продолжается
                if previous_interaction_id and environment_id:
                    kwargs["previous_interaction_id"] = previous_interaction_id
                    kwargs["environment"] = environment_id
                    kwargs["input"] = current_input_prompt
                else:
                    # Новая сессия или Handoff
                    kwargs["system_instruction"] = agent_system_instruction
                    kwargs["environment"] = "remote"

                    # Если это эстафета (Handoff с историей диалога)
                    if recent_history:
                        history_parts = []
                        for m in recent_history[-8:]:
                            role = "Пользователь" if m.get("role") == "user" else "Ассистент"
                            text_snip = (m.get("text") or "").strip()
                            if text_snip:
                                if len(text_snip) > 400: text_snip = text_snip[:400] + "..."
                                history_parts.append(f"{role}: {text_snip}")
                        
                        if history_parts:
                            history_block = "\n".join(history_parts)
                            current_input_prompt = (
                                f"# Продолжение диалога (сессия переведена на рабочий инстанс)\n\n"
                                f"Краткий контекст недавних действий:\n{history_block}\n\n"
                                f"Текущая задача:\n{prompt_text}"
                            )

                    kwargs["input"] = current_input_prompt

                interaction = client.interactions.create(**kwargs)
                new_interaction_id = interaction.id
                env_id = interaction.environment_id

                self.antigravity_sessions[new_interaction_id] = assigned_key
                if previous_interaction_id:
                    self.antigravity_sessions[previous_interaction_id] = assigned_key

                if status_callback:
                    await status_callback({"status": "started", "interaction_id": new_interaction_id})

                while True:
                    await asyncio.sleep(3)
                    interaction = client.interactions.get(id=new_interaction_id)
                    
                    current_step_desc = interaction.status
                    try:
                        if hasattr(interaction, 'events') and interaction.events:
                            last_event = interaction.events[-1]
                            if hasattr(last_event, 'content') and last_event.content:
                                current_step_desc = str(last_event.content)[:90]
                            elif hasattr(last_event, 'call') and last_event.call:
                                current_step_desc = f"Запуск: {last_event.call.name}"
                        elif hasattr(interaction, 'steps') and interaction.steps:
                            current_step_desc = str(interaction.steps[-1])[:90]
                    except Exception: pass

                    if status_callback:
                        await status_callback({"status": current_step_desc})

                    if interaction.status in ("completed", "failed"):
                        break

                if interaction.status == "failed":
                    raise Exception(f"Задача Antigravity завершилась ошибкой: {interaction.error}")

                output_text = interaction.output_text or ""
                import re
                sync_matches = re.findall(r"\[SYNC:\s*([^\]]+)\]", output_text)
                file_urls = {}

                save_dir = os.path.join('/opt/friday/storage/uploads', chat_uid or 'common')
                os.makedirs(save_dir, exist_ok=True)

                real_files_in_env = []
                try:
                    list_resp = client.environments.files.list(environment=env_id, path="")
                    real_files_in_env = [f.name for f in list_resp.files if f.name not in ('.', '..', 'usr')]
                    logger.info(f"[Antigravity Env Files] Реальные файлы в контейнере: {real_files_in_env}")
                except Exception as le:
                    logger.warning(f"[Antigravity Env Files] Ошибка: {le}")

                for sync_path in sync_matches:
                    clean_path = sync_path.strip().replace("\\", "/").lstrip("./")
                    fname = os.path.basename(clean_path)
                    download_success = False

                    candidate_paths = [clean_path, f"workspace/{clean_path}"]
                    for rf in real_files_in_env:
                        if rf.endswith(fname) and rf not in candidate_paths:
                            candidate_paths.append(rf)

                    for cp in candidate_paths:
                        try:
                            file_data = client.environments.files.download(environment=env_id, path=cp)
                            content = file_data.read() if hasattr(file_data, "read") else file_data
                            if hasattr(content, "content"): content = content.content

                            local_server_path = os.path.join(save_dir, clean_path)
                            os.makedirs(os.path.dirname(local_server_path), exist_ok=True)
                            with open(local_server_path, 'wb') as sf:
                                sf.write(content)

                            file_url = f"https://friday-assistant.ru/uploads/{chat_uid or 'common'}/{urllib.parse.quote(clean_path)}"
                            file_urls[clean_path] = file_url
                            logger.info(f"[Antigravity Sync] Файл {fname} ({len(content)} байт) выкачан -> {file_url}")
                            download_success = True
                            break
                        except Exception: continue

                    if not download_success:
                        logger.warning(f"[Antigravity Sync] Файл {clean_path} НЕ найден в контейнере: {real_files_in_env}")

                return {
                    "text": output_text,
                    "files": {},
                    "file_urls": file_urls,
                    "environment_id": env_id,
                    "interaction_id": new_interaction_id,
                    "assigned_key": assigned_key
                }

            except Exception as e:
                err_str = str(e)
                err_lower = err_str.lower()
                logger.warning(f"[Antigravity Key #{assigned_key}] Ошибка: {err_str}")

                # 1. Если новый ключ не имеет прав на старое окружение другого аккаунта
                if ("not found" in err_lower or "permission" in err_lower or "403" in err_lower or "404" in err_lower) and previous_interaction_id:
                    logger.info(f"[Antigravity Handoff] Ключ #{assigned_key} не видит среду #{environment_id}. Делаем Handoff на чистую среду.")
                    previous_interaction_id = None
                    environment_id = None
                    attempt += 1
                    continue

                # 2. Ошибка квот и лимитов (429 / ResourceExhausted)
                if "429" in err_str or "too_many_requests" in err_lower or "resource_exhausted" in err_lower:
                    is_daily = ("per day" in err_lower or "queries per day" in err_lower or "daily" in err_lower or "limit: 100" in err_lower)
                    
                    if is_daily or total_keys > 1:
                        self.key_pool.mark_exhausted(assigned_key, duration_seconds=3600 * 12)
                        next_key = self.key_pool.get_available_key(assigned_key)
                        if next_key != assigned_key:
                            logger.info(f"[Antigravity Switch] Ключ #{assigned_key} уперся в лимит. Переключаем на #{next_key}...")
                            if status_callback:
                                await status_callback({"status": f"Лимит ключа #{assigned_key}, переключение на ключ #{next_key}..."})
                            assigned_key = next_key
                            attempt += 1
                            continue

                    wait_seconds = 10
                    import re
                    m = re.search(r"retry in ([\d\.]+)s", err_str)
                    if m:
                        try: wait_seconds = int(float(m.group(1))) + 2
                        except: pass

                    logger.info(f"[Antigravity TPM] Пауза {wait_seconds}с на ключе #{assigned_key}...")
                    if status_callback:
                        await status_callback({"status": f"Пауза {wait_seconds}с (TPM)..."})
                    await asyncio.sleep(wait_seconds)
                    attempt += 1
                    continue

                # Любая другая ошибка
                attempt += 1
                if attempt < max_attempts and total_keys > 1:
                    assigned_key = self.key_pool.get_available_key(assigned_key)
                    continue
                raise e

        raise Exception(f"Antigravity: исчерпаны попытки после перебора ключей. Последний ключ: #{assigned_key}")

ai_instance = AIService()
