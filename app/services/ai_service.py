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
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AI_Service")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

try:
    from app.config.secrets import GEMINI_KEYS
except ImportError:
    GEMINI_KEYS = []

try:
    from app.config.settings import PROXY_URL
except ImportError:
    PROXY_URL = os.environ.get('PROXY_URL') or os.environ.get('HTTPS_PROXY') or os.environ.get('HTTP_PROXY') or ''

if PROXY_URL:
    logger.info(f"🌐 [AI_Service] Используется PROXY: {PROXY_URL.split('@')[-1] if '@' in PROXY_URL else PROXY_URL}")
else:
    logger.info("🌐 [AI_Service] Прямое подключение без прокси (PROXY_URL не задан)")

class GeminiKeyPool:
    def __init__(self, keys: list[str]):
        self.keys = keys or []
        self.exhausted_keys = {}
        self._round_robin_counter = 0

    def mark_exhausted(self, key_index: int, duration_seconds: float = 3600):
        now = time.time()
        self.exhausted_keys[key_index] = now + duration_seconds
        logger.warning(f"🚫 [Key Pool] Ключ #{key_index} помечен как НЕРАБОЧИЙ на {duration_seconds/60:.0f} мин.")

    def is_usable(self, key_index: int) -> bool:
        if not self.keys: return True
        if key_index not in self.exhausted_keys: return True
        if time.time() > self.exhausted_keys[key_index]:
            del self.exhausted_keys[key_index]
            return True
        return False

    def get_next_key(self, exclude_index: int = None) -> int:
        """Честный Round-Robin: каждый раз выдает СЛЕДУЮЩИЙ живой ключ из пула."""
        total = len(self.keys)
        if total <= 1: return 0

        for _ in range(total):
            self._round_robin_counter = (self._round_robin_counter + 1) % total
            candidate = self._round_robin_counter
            if exclude_index is not None and candidate == exclude_index:
                continue
            if self.is_usable(candidate):
                return candidate

        # Если все ключи в бане — сбрасываем самый ранний
        if self.exhausted_keys:
            earliest = min(self.exhausted_keys.keys(), key=lambda k: self.exhausted_keys[k])
            del self.exhausted_keys[earliest]
            return earliest
        return 0

    def get_available_key(self, exclude_index: int = None) -> int:
        return self.get_next_key(exclude_index=exclude_index)

class AIService:
    def __init__(self):
        self.api_keys = GEMINI_KEYS
        self.current_key_index = 0
        self.antigravity_sessions = {}
        self.key_pool = GeminiKeyPool(self.api_keys)

    def _get_http_options(self, timeout=60000):
        client_args = {}
        async_client_args = {}
        if PROXY_URL:
            client_args["proxy"] = PROXY_URL
            async_client_args["proxy"] = PROXY_URL

        return types.HttpOptions(
            api_version="v1beta",
            timeout=timeout,
            client_args=client_args or None,
            async_client_args=async_client_args or None
        )

    def _get_client(self, key_index=None):
        idx = key_index if key_index is not None else self.current_key_index
        api_key = self.api_keys[idx] if self.api_keys and idx < len(self.api_keys) else None
        return genai.Client(
            http_options=self._get_http_options(timeout=60000),
            api_key=api_key
        )

    def _get_live_client(self):
        live_key = self.api_keys[0] if self.api_keys else None
        return genai.Client(
            http_options=self._get_http_options(timeout=None),
            api_key=live_key
        )

    def _rotate_key(self):
        if len(self.api_keys) <= 1: return False
        self.current_key_index = self.key_pool.get_next_key(exclude_index=self.current_key_index)
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
                if response.server_content and response.data: audio_data.extend(response.data)
                if response.server_content and response.server_content.turn_complete: break
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
                if response.server_content and response.data: yield response.data
                if response.server_content and response.server_content.turn_complete: break
        except Exception as e:
            logger.error(f"[TTS STREAM ERROR]: {e}")
        finally:
            if session:
                try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                except: pass

    def _build_device_control_tool(self, allowed_actions):
        if isinstance(allowed_actions, dict):
            action_names = list(allowed_actions.keys())
            descriptions = "; ".join([f"'{k}' ({v})" for k, v in allowed_actions.items()])
            value_desc = f"Параметр действия. Формат значений: {descriptions}"
        elif isinstance(allowed_actions, list):
            action_names = allowed_actions
            value_desc = "Значение/параметр действия"
        else:
            action_names = [a.strip() for a in str(allowed_actions).split(",") if a.strip()]
            value_desc = "Значение/параметр действия"

        return types.Tool(
            function_declarations=[
                types.FunctionDeclaration(
                    name="send_device_commands",
                    description="Отправляет системные команды на целевые устройства пользователя (ПК, телефон, робот, веб).",
                    parameters=types.Schema(
                        type=types.Type.OBJECT,
                        properties={
                            "target_device": types.Schema(
                                type=types.Type.STRING, 
                                description="Имя целевого устройства из списка онлайн-устройств"
                            ),
                            "actions": types.Schema(
                                type=types.Type.ARRAY,
                                description="Список действий для выполнения на целевом устройстве",
                                items=types.Schema(
                                    type=types.Type.OBJECT,
                                    properties={
                                        "action_type": types.Schema(
                                            type=types.Type.STRING, 
                                            enum=action_names if action_names else None,
                                            description="Тип выполняемого действия"
                                        ),
                                        "action_value": types.Schema(
                                            type=types.Type.STRING, 
                                            description=value_desc
                                        )
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

    # ==============================================================================
    # 2. ФАСАД (Gemini 3.1 Flash Live) — ЕДИНЫЙ ГОЛОСОВОЙ ИНТЕРФЕЙС
    # ==============================================================================
    async def generate_audio_stream(self, prompt_text, system_instruction, allowed_actions, audio_bytes=None, image_bytes=None, formatted_history=None, voice_name="Aoede", assistant_name="Пятница", media_queue=None):
        voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
        valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
        mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"
        has_yielded_data = False 
        
        try:
            client = self._get_live_client()
            device_control_tool = self._build_device_control_tool(allowed_actions)

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
                        if prompt_text: await session.send_realtime_input(text=prompt_text)
                        if image_bytes: await session.send_realtime_input(video=types.Blob(data=image_bytes, mime_type="image/jpeg"))

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

                had_tool_call = False
                had_bot_output = False

                while True:
                    response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=30.0)
                    if response.tool_call:
                        had_tool_call = True
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
                            had_bot_output = True
                            yield {"type": "bot_text", "text": sc.output_transcription.text}
                        if sc.model_turn:
                            for part in sc.model_turn.parts:
                                if part.inline_data:
                                    has_yielded_data = True
                                    had_bot_output = True
                                    yield {"type": "audio", "data": part.inline_data.data}
                        if sc.turn_complete:
                            if had_tool_call and not had_bot_output:
                                continue
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
        
        session_audio_cache = bytearray()
        last_video_frame = None
        has_reached_stream_end = False
        has_yielded_data = False 
        
        try:
            client = self._get_live_client()
            device_control_tool = self._build_device_control_tool(allowed_actions)

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

                had_tool_call = False
                had_bot_output = False

                while True:
                    response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=30.0)
                    if response.tool_call:
                        had_tool_call = True
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
                            had_bot_output = True
                            yield {"type": "bot_text", "text": sc.output_transcription.text}
                        if sc.model_turn:
                            for part in sc.model_turn.parts:
                                if part.inline_data:
                                    has_yielded_data = True
                                    had_bot_output = True
                                    yield {"type": "audio", "data": part.inline_data.data}
                        if sc.turn_complete:
                            if had_tool_call and not had_bot_output:
                                continue
                            break
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
    # 3. АВТОНОМНЫЙ АГЕНТ ANTIGRAVITY С КОРРЕКТНЫМ HANDOFF И РОТАЦИЕЙ
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

        if assigned_key is None or not self.key_pool.is_usable(assigned_key):
            assigned_key = self.key_pool.get_next_key(exclude_index=assigned_key)
            previous_interaction_id = None
            environment_id = None

        agent_system_instruction = """Ты — автономный Senior разработчик.
Все файлы проекта (код, верстку, скрипты, презы, 3D-модели, Word-документы) ты создаешь, изменяешь и тестируешь прямо в своем окружении Linux.

Твое взаимодействие с компьютером пользователя строится через теги:
1. [GET_TREE] — запросить дерево проекта у пользователя.
2. [GET_FILES: путь1, путь2] — запросить содержимое файлов с компьютера пользователя.
3. [SYNC: относительный_путь] — синхронизировать созданный или измененный тобой файл на компьютер пользователя. 
   Сначала создай/отредактируй файл в своей файловой системе, протестируй его, а затем укажи [SYNC: путь].
4. [RUN_COMMAND: команда] — выполнить системную команду на компьютере пользователя (Windows). Используй `python`, а не `python3`.
5. [ASK_USER: Текст вопроса | Вариант 1 | Вариант 2 | Вариант 3] — уточнить параметры задачи или спросить мнение пользователя.
6. [DELETE: относительный_путь] — удалить файл или папку на компьютере пользователя.

ПРАВИЛА:
- На простые вопросы отвечай текстом без тегов.
- ЗАПРЕЩЕНО выводить полные тексты файлов в чат! Создавай файлы в файловой системе и используй [SYNC: путь].
"""

        max_attempts = min(total_keys, 10) if total_keys > 0 else 3
        attempt = 0

        while attempt < max_attempts:
            client = self._get_client(key_index=assigned_key)

            try:
                if status_callback:
                    await status_callback({"status": f"Подключение к Google (ключ #{assigned_key})..."})

                kwargs = {
                    "agent": "antigravity-preview-09-2026",
                    "background": True,
                }

                current_input_prompt = prompt_text

                if previous_interaction_id and environment_id:
                    kwargs["previous_interaction_id"] = previous_interaction_id
                    kwargs["environment"] = environment_id
                    kwargs["input"] = current_input_prompt
                    logger.info(f"🔗 [GOOGLE CALL] Продолжение среды {environment_id[:10]} | Ключ #{assigned_key}")
                else:
                    kwargs["system_instruction"] = agent_system_instruction
                    kwargs["environment"] = "remote"

                    if recent_history:
                        history_parts = []
                        for m in recent_history[-8:]:
                            role = "Пользователь" if m.get("role") == "user" else "Ассистент"
                            text_snip = (m.get("text") or "").strip()
                            if text_snip:
                                if len(text_snip) > 300: text_snip = text_snip[:300] + "..."
                                history_parts.append(f"{role}: {text_snip}")
                        
                        if history_parts:
                            current_input_prompt = (
                                f"# История недавних сообщений диалога:\n" + "\n".join(history_parts) + f"\n\n# Текущее задание:\n{prompt_text}"
                            )
                    kwargs["input"] = current_input_prompt
                    logger.info(f"✨ [GOOGLE CALL] Старт нового окружения на ключе #{assigned_key}")

                interaction = await asyncio.wait_for(
                    asyncio.to_thread(client.interactions.create, **kwargs),
                    timeout=65.0
                )
                
                new_interaction_id = interaction.id
                env_id = interaction.environment_id
                logger.info(f"🆔 [GOOGLE CREATED] interaction_id={new_interaction_id} | env_id={env_id}")

                if status_callback:
                    await status_callback({"status": "Агент думает (контейнер запущен)...", "interaction_id": new_interaction_id})

                while True:
                    await asyncio.sleep(2.5)
                    interaction = await asyncio.wait_for(
                        asyncio.to_thread(client.interactions.get, id=new_interaction_id),
                        timeout=25.0
                    )
                    
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
                sync_matches = re.findall(r"\[SYNC:\s*([^\]]+)\]", output_text)
                file_urls = {}

                save_dir = os.path.join('/opt/friday/storage/uploads', chat_uid or 'common')
                os.makedirs(save_dir, exist_ok=True)

                real_files_in_env = []
                try:
                    list_resp = client.environments.files.list(environment=env_id, path="")
                    real_files_in_env = [f.name for f in list_resp.files if f.name not in ('.', '..', 'usr')]
                except Exception: pass

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
                            download_success = True
                            break
                        except Exception: continue

                return {
                    "text": output_text,
                    "files": {},
                    "file_urls": file_urls,
                    "environment_id": env_id,
                    "interaction_id": new_interaction_id,
                    "assigned_key": assigned_key
                }

            except (asyncio.TimeoutError, TimeoutError) as te:
                logger.warning(f"⏰ [ТАЙМАУТ] Ключ #{assigned_key} не ответил за 60с. Пробуем следующий...")
                self.key_pool.mark_exhausted(assigned_key, duration_seconds=300)
                assigned_key = self.key_pool.get_next_key(exclude_index=assigned_key)
                previous_interaction_id = None
                environment_id = None
                attempt += 1
                continue

            except Exception as e:
                err_str = str(e)
                err_lower = err_str.lower()
                logger.warning(f"[Antigravity Ошибка на ключе #{assigned_key}]: {err_str}")

                if "timed out" in err_lower or "timeout" in err_lower:
                    self.key_pool.mark_exhausted(assigned_key, duration_seconds=300)
                    assigned_key = self.key_pool.get_next_key(exclude_index=assigned_key)
                    previous_interaction_id = None
                    environment_id = None
                    attempt += 1
                    continue

                if ("not found" in err_lower or "permission" in err_lower or "403" in err_lower or "404" in err_lower) and (previous_interaction_id or environment_id):
                    logger.info(f"🔄 Сброс чужой среды #{environment_id} для ключа #{assigned_key}.")
                    previous_interaction_id = None
                    environment_id = None
                    attempt += 1
                    continue

                self.key_pool.mark_exhausted(assigned_key, duration_seconds=3600)
                next_key = self.key_pool.get_next_key(exclude_index=assigned_key)
                logger.info(f"Переход с ключа #{assigned_key} на #{next_key}...")
                assigned_key = next_key
                previous_interaction_id = None
                environment_id = None
                attempt += 1
                continue

        raise Exception(f"Все доступные ключи не отвечают или исчерпали квоту.")

ai_instance = AIService()
