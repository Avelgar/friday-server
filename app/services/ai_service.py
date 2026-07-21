# -*- coding: utf-8 -*-
import base64
import asyncio
import logging
import json
import websockets
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
        self.api_robot_keys = self.api_keys[:30] if len(self.api_keys) >= 30 else self.api_keys
        self.current_key_index = 0

    def _get_client(self):
        return genai.Client(http_options={"api_version": "v1beta"}, api_key=self.api_keys[self.current_key_index])

    def _rotate_key(self):
        if len(self.api_keys) <= 1: return False
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        return True

    # Статическое аудио для удаленных устройств (через стабильный SDK)
    async def generate_static_audio(self, text, voice_name="Aoede", assistant_name="Пятница"):
        mapped_voice = "Puck" if voice_name.lower() in ["dmitri", "dmitry", "puck"] else "Aoede"
        client = self._get_client()
        config = types.LiveConnectConfig(
            response_modalities=["AUDIO"], 
            speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice)))
        )
        audio_data = bytearray()
        try:
            async with client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config) as session:
                await session.send(input=f"Произнеси: {text}", end_of_turn=True)
                async for response in session.receive():
                    if response.server_content and response.data:
                        audio_data.extend(response.data)
            return base64.b64encode(audio_data).decode('utf-8') if audio_data else None
        except Exception as e:
            logger.error(f"Static audio error: {e}")
            return None

    # ГЛАВНЫЙ ГЕНЕРАТОР ПОТОКА LIVE API (SDK)
    async def generate_audio_stream(self, prompt_text, audio_bytes=None, image_bytes=None, history_text="", voice_name="Aoede", assistant_name="Пятница"):
        mapped_voice = "Puck" if voice_name.lower() in ["dmitri", "dmitry", "puck"] else "Aoede"

        total_keys_tried = 0
        while total_keys_tried < len(self.api_keys):
            if total_keys_tried > 0: # Rotate only after the first attempt
                self._rotate_key()
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
                                    "target_device": types.Schema(type=types.Type.STRING, description="Имя устройства СТРОГО из списка доступных."),
                                    "actions": types.Schema(
                                        type=types.Type.ARRAY,
                                        items=types.Schema(
                                            type=types.Type.OBJECT,
                                            properties={
                                                "action_type": types.Schema(type=types.Type.STRING, description="Тип действия"),
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
                
                system_instruction = f"""Ты — ИИ-помощник {assistant_name}.
ПРАВИЛА ОБЩЕНИЯ И УПРАВЛЕНИЯ:
1. Ты общаешься ТОЛЬКО ГОЛОСОМ. Говори естественно и кратко.
2. Используй функцию send_device_commands ТОЛЬКО для управления устройствами (если пользователь просит что-то включить, открыть, настроить, получить данные).
3. **НИКОГДА не вызывай send_device_commands с action_type="голосовой ответ" для устройства-отправителя!** Твой голос автоматически транслируется ему.
4. Если нужно сказать что-то на ДРУГОМ устройстве, тогда используй send_device_commands с action_type="голосовой ответ" и нужным текстом.
5. Устройства:
   [КОМПЬЮТЕР]: открытие ссылки (принимает ссылку), напечатать текст (принимает текст, используй \\n для переноса строки), нажать кнопку мыши (лкм/пкм/скм), переместить мышь (x,y), уведомление (принимает текст), музыка(включить/выключить/следующий/предыдущий), смена имени(новое имя), смена голоса(Irina/Dmitri), очистка истории(любой текст), изменение громкости(число от 0 до 100), изменение яркости(число от 0 до 100), data_request (paths_to_programs/running_processes/need_repeat).
   [ТЕЛЕФОН]: открытие ссылки(принимает ссылку), изменение громкости(число от 0 до 100), изменение яркости(число от 0 до 100), музыка, очистка истории(любой текст), режим камеры(любой текст), выключить режим камеры(любой текст), data_request (paths_to_programs/running_processes).

ИСТОРИЯ ДИАЛОГА:
{history_text}"""

                config = types.LiveConnectConfig(
                    response_modalities=["AUDIO"], 
                    system_instruction=types.Content(parts=[types.Part.from_text(text=system_instruction)]),
                    tools=[device_control_tool],
                    input_audio_transcription={},  # Включаем транскрипцию голоса пользователя
                    output_audio_transcription={}, # Включаем транскрипцию ответа бота
                    speech_config=types.SpeechConfig(
                        voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice))
                    )
                )

                logger.info(f"[CONNECT] Подключаюсь к Live API (SDK, ключ {self.current_key_index})...")
                async with client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config) as session:
                    
                    # 1. ОТПРАВЛЯЕМ ТЕКСТ (ТОЛЬКО ЕСЛИ ОН ЕСТЬ)
                    if prompt_text:
                        await session.send_realtime_input(text=prompt_text)
                    
                    # 2. ОТПРАВЛЯЕМ КАРТИНКУ (ТОЛЬКО ЕСЛИ ОНА ЕСТЬ)
                    if image_bytes:
                        await session.send_realtime_input(video=types.Blob(data=image_bytes, mime_type="image/jpeg"))

                    # 3. ОТПРАВЛЯЕМ АУДИО
                    if audio_bytes:
                        pcm_data = audio_bytes[44:] if audio_bytes.startswith(b'RIFF') else audio_bytes
                        await session.send_realtime_input(audio=types.Blob(data=pcm_data, mime_type="audio/pcm;rate=16000"))

                    # 4. Сообщаем, что мы закончили говорить
                    await session.send_realtime_input(audio_stream_end=True)

                    # 5. ЧИТАЕМ ОТВЕТЫ (Пока сессия активна)
                    async for response in session.receive():
                        sc = response.server_content
                        if sc:
                            # А. Транскрипция пользователя
                            if sc.input_transcription:
                                yield {"type": "user_text", "text": sc.input_transcription.text}

                            # Б. Транскрипция ответа бота (Текст для чата)
                            if sc.output_transcription:
                                yield {"type": "bot_text", "text": sc.output_transcription.text}

                            # В. Аудио бота
                            if sc.model_turn:
                                for part in sc.model_turn.parts:
                                    if part.inline_data:
                                        yield {"type": "audio", "data": part.inline_data.data}

                            # Г. Флаг завершения хода (Просто логируем)
                            if sc.turn_complete:
                                logger.info("[API] Модель завершила свою реплику (turn_complete).")
                        
                        # Д. Вызов инструментов
                        if response.tool_call:
                            extracted_commands = []
                            function_responses = []
                            
                            for fc in response.tool_call.function_calls:
                                args_dict = type(fc.args).to_dict(fc.args) if hasattr(fc.args, 'to_dict') else dict(fc.args)
                                if isinstance(args_dict, dict) and "actions" in args_dict:
                                    extracted_commands.append(args_dict)
                                function_responses.append(types.FunctionResponse(name=fc.name, id=fc.id, response={"result": "OK"}))
                            
                            if extracted_commands:
                                yield {"type": "commands", "commands": extracted_commands}
                            
                            await session.send_tool_response(function_responses=function_responses)
                
                # Если цикл `async for` штатно завершился, выходим из `while` ротации ключей
                return

            except Exception as e:
                logger.warning(f"[API ERROR] Ошибка Live API на ключе {self.current_key_index} (попытка {total_keys_tried + 1}/{len(self.api_keys)}): {e}")
                total_keys_tried += 1
                if total_keys_tried < len(self.api_keys):
                    await asyncio.sleep(1) # Wait before retrying with a new key
                else:
                    break # All keys tried, exit loop

        raise Exception("AI Live Service Unavailable")

ai_instance = AIService()