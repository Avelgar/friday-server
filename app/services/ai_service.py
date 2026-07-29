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
        Синхронная функция генерации картинок Imagen 4. 
        Принимает prompt и model_type (fast, generate, ultra).
        """
        models_map = {
            "fast": "models/imagen-4.0-fast-generate-001",
            "generate": "models/imagen-4.0-generate-001",
            "ultra": "models/imagen-4.0-ultra-generate-001"
        }
        model_id = models_map.get(model_type, models_map["generate"])

        config = {
            "number_of_images": 1,
            "person_generation": "ALLOW_ADULT",
            "aspect_ratio": "1:1",
        }
        
        # Fast модель не принимает image_size, для остальных ставим 1K
        if model_type != "fast":
            config["image_size"] = "1K"

        total_keys_tried = 0
        last_error_msg = ""
        
        while total_keys_tried < len(self.api_keys):
            self._rotate_key()
            try:
                client = self._get_client()
                
                logger.info(f"[IMAGE GEN] Попытка генерации {model_type} (Ключ: {self.current_key_index})")
                
                result = client.models.generate_images(
                    model=model_id,
                    prompt=prompt,
                    config=config
                )

                if not result.generated_images:
                    raise Exception("Изображения не были сгенерированы (возможно блок фильтров безопасности).")

                generated_image = result.generated_images[0]
                image_data = None

                # Пытаемся извлечь байты (зависит от версии библиотеки google-genai)
                if hasattr(generated_image.image, 'image_bytes') and generated_image.image.image_bytes:
                    image_data = generated_image.image.image_bytes
                else:
                    # Фоллбек через BytesIO (если SDK возвращает PIL Image или объект с методом save)
                    buffered = io.BytesIO()
                    # Сохраняем в память (вместо файла)
                    generated_image.image.save(buffered, format="JPEG")
                    image_data = buffered.getvalue()

                if not image_data:
                    raise Exception("Не удалось получить байты изображения")

                return base64.b64encode(image_data).decode('utf-8')

            except Exception as e:
                logger.warning(f"[IMAGE GEN ERROR] Ошибка на ключе {self.current_key_index}: {e}")
                last_error_msg = str(e)
                total_keys_tried += 1
                
        raise Exception(f"AI Image Service недоступен (Перебраны все ключи). Последняя ошибка: {last_error_msg}")

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