# -*- coding: utf-8 -*-
import base64
import asyncio
import logging
import json
import io
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
        """
        Генерация картинок через новые модели Gemini Image (например, gemini-2.5-flash-image)
        с использованием метода generate_content вместо устаревшего predict.
        """
        # Маппинг на новые модели, которые действительно поддерживаются вашими ключами
        models_map = {
            "fast": "gemini-3.1-flash-lite-image",
            "generate": "gemini-2.5-flash-image",
            "ultra": "gemini-3-pro-image"
        }
        model_id = models_map.get(model_type, models_map["generate"])

        total_keys_tried = 0
        last_error_msg = ""
        
        while total_keys_tried < len(self.api_keys):
            self._rotate_key()
            try:
                client = self._get_client()
                
                logger.info(f"[IMAGE GEN] Генерация через {model_id} (Ключ индекс: {self.current_key_index})")
                
                # Новые модельные ряды gemini-*-image используют generate_content
                response = client.models.generate_content(
                    model=model_id,
                    contents=prompt,
                    # Можно указать конфигурацию, если требуется, но для дефолта достаточно contents
                )

                image_bytes = None

                # Ищем инлайн-данные (изображение) в ответе модели
                if response.candidates:
                    for candidate in response.candidates:
                        if candidate.content and candidate.content.parts:
                            for part in candidate.content.parts:
                                # Проверяем, есть ли картинка в байтах или в file_data/inline_data
                                if hasattr(part, 'inline_data') and part.inline_data and part.inline_data.data:
                                    image_bytes = part.inline_data.data
                                    break
                        if image_bytes:
                            break

                # Альтернативный вариант поиска байтов через sdk-структуры
                if not image_bytes and hasattr(response, 'inline_data') and response.inline_data:
                    image_bytes = response.inline_data.data

                # Если модель вернула картинку в тексте как base64 или иным способом (на всякий случай)
                if not image_bytes and response.text:
                    # Некоторые версии могут вернуть текст, попробуем забрать из кандидатов напрямую
                    pass

                if not image_bytes:
                    # Попробуем пройтись по всем частям ответа универсально
                    try:
                        for part in response.parts:
                            if part.inline_data:
                                image_bytes = part.inline_data.data
                                break
                    except Exception:
                        pass

                if not image_bytes:
                    raise Exception(f"Модель {model_id} не вернула байты изображения в ответе.")

                return base64.b64encode(image_bytes).decode('utf-8')

            except Exception as e:
                logger.warning(f"[IMAGE GEN ERROR] Ошибка на ключе {self.current_key_index}: {e}")
                last_error_msg = str(e)
                total_keys_tried += 1
                
        raise Exception(f"AI Image Service недоступен. Последняя ошибка: {last_error_msg}")

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
                if response.server_content and response.data:
                    audio_data.extend(response.data)
        except (asyncio.TimeoutError, TimeoutError):
            logger.error(f"Static audio timeout on key {self.current_key_index}")
            return None
        except StopAsyncIteration:
            pass
        except Exception as e:
            logger.error(f"Static audio error: {e}")
            return None
        finally:
            if session:
                try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                except: pass
                
        return base64.b64encode(audio_data).decode('utf-8') if audio_data else None

    async def generate_audio_stream(self, prompt_text, system_instruction, allowed_actions, audio_bytes=None, image_bytes=None, history_text="", voice_name="Aoede", assistant_name="Пятница"):
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
                                                "action_value": types.Schema(type=types.Type.STRING, description="Значение (полная ссылка, текст или пусто)")
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
                    input_audio_transcription={},  
                    output_audio_transcription={}, 
                    speech_config=types.SpeechConfig(
                        voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice))
                    )
                )

                logger.info(f"[CONNECT] Подключаюсь к Live API (SDK, ключ {self.current_key_index})...")
                
                cm = client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config)
                session = None
                
                try:
                    session = await asyncio.wait_for(cm.__aenter__(), timeout=10.0)
                    
                    if prompt_text:
                        await session.send_realtime_input(text=prompt_text)
                    if image_bytes:
                        await session.send_realtime_input(video=types.Blob(data=image_bytes, mime_type="image/jpeg"))
                    if audio_bytes:
                        pcm_data = audio_bytes[44:] if audio_bytes.startswith(b'RIFF') else audio_bytes
                        await session.send_realtime_input(audio=types.Blob(data=pcm_data, mime_type="audio/pcm;rate=16000"))

                    await session.send_realtime_input(audio_stream_end=True)

                    receive_iterator = session.receive().__aiter__()
                    while True:
                        response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=25.0)
                        
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
                                logger.info("[API] Модель завершила свою реплику (turn_complete).")
                        
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
                    raise Exception("Таймаут подключения/стрима (зависание до начала ответа)")
                except StopAsyncIteration:
                    pass
                finally:
                    if session:
                        try: await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                        except: pass
                
                # === ЖЕСТКАЯ ПРОВЕРКА НА ПУСТОЙ ОТВЕТ ===
                if not has_yielded_data:
                    raise Exception("Gemini вернул абсолютно пустой ответ (нет текста, аудио и команд)")
                
                return # Успешно отработали, выходим

            except Exception as e:
                if has_yielded_data:
                    logger.info(f"[API] Обрыв связи во время ответа ({e}). Считаем завершенным, не повторяем.")
                    return
                
                logger.warning(f"[API ERROR] Ошибка/Таймаут на ключе {self.current_key_index}: {e}")
                total_keys_tried += 1
                if total_keys_tried < len(self.api_keys):
                    await asyncio.sleep(1)
                else:
                    break

        raise Exception("AI Live Service Unavailable (Все ключи перебраны или недоступны)")

ai_instance = AIService()