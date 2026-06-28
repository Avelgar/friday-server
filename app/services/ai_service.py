import base64
import asyncio
import logging
import time
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

# Настройка логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AI_Service")

try:
    from app.config.secrets import GEMINI_KEYS
except ImportError:
    GEMINI_KEYS = []

# =====================================================================
# --- 1. ОПИСАНИЕ СТРУКТУРЫ КОМАНД ДЛЯ ИИ ЧЕРЕЗ PYDANTIC (АВТО-СХЕМА) ---
# =====================================================================

class DeviceAction(BaseModel):
    action_type: str = Field(
        description="Тип действия. Доступные значения: 'открытие ссылки', 'напечатать текст', 'нажать кнопку мыши', 'переместить мышь', 'уведомление', 'музыка', 'погода', 'смена имени', 'смена голоса', 'очистка истории', 'скриншот', 'режим камеры', 'выключить режим камеры', 'изменение громкости', 'изменение яркости', 'data_request'"
    )
    action_value: str = Field(
        description="Параметр действия. ВАЖНО: для действия 'нажать кнопку мыши' разрешены ТОЛЬКО значения: 'лкм' (левая кнопка), 'пкм' (правая кнопка), 'скм' (средняя кнопка). Для 'data_request' разрешены только: 'running_processes', 'paths_to_programs', 'need_repeat'. Для остальных: URL ссылки, текст для печати и т.д."
    )

def send_device_commands(target_device: str, actions: list[DeviceAction]):
    """
    Отправляет одну или несколько команд на указанное устройство пользователя.

    Args:
        target_device: Имя устройства СТРОГО из списка доступных (например: 'Компьютер', 'Телефон'). Не добавляй от себя слов вроде 'Кирилла' или 'пользователя'.
        actions: Список действий для этого устройства.
    """
    pass


# =====================================================================


class AIService:
    def __init__(self):
        self.api_keys = GEMINI_KEYS

        self.models = ["gemini-2.5-flash"]
        self.api_robot_keys = self.api_keys[:30] if len(self.api_keys) >= 30 else self.api_keys
        self.robot_models = ["gemini-3.1-flash-lite-preview"]

        self.current_key_index = 0
        self.current_robot_key_index = 0
    def _get_client(self, is_robot=False):
        key = self.api_robot_keys[self.current_robot_key_index] if is_robot else self.api_keys[self.current_key_index]
        return genai.Client(http_options={"api_version": "v1beta"}, api_key=key)

    def _rotate_key(self, is_robot=False):
        if is_robot:
            if len(self.api_robot_keys) <= 1: return False
            self.current_robot_key_index = (self.current_robot_key_index + 1) % len(self.api_robot_keys)
        else:
            if len(self.api_keys) <= 1: return False
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        return True

    def generate_content(self, contents):
        total_keys_tried = 0
        while total_keys_tried < len(self.api_keys):
            try:
                client = self._get_client(is_robot=False)
                return client.models.generate_content(model=self.models[0], contents=contents)
            except Exception as e:
                logger.warning(f"Ошибка на ключе {self.current_key_index}: {e}")
                if self._rotate_key(is_robot=False):
                    total_keys_tried += 1
                    time.sleep(0.5)
                else:
                    break
        raise Exception("AI Service Unavailable")

    def generate_content_robot(self, contents):
        total_keys_tried = 0
        while total_keys_tried < len(self.api_robot_keys):
            try:
                client = self._get_client(is_robot=True)
                return client.models.generate_content(model=self.robot_models[0], contents=contents)
            except Exception as e:
                logger.warning(f"Ошибка на ключе {self.current_robot_key_index}: {e}")
                if self._rotate_key(is_robot=True):
                    total_keys_tried += 1
                    time.sleep(0.5)
                else:
                    break
        raise Exception("AI Robot Service Unavailable")
        
    def generate_image(self, prompt: str) -> str:
        """
        Генерирует изображение через gemini-3.1-flash-image.
        Использует строго один указанный Tier-1 ключ.
        """
        tier1_key = "AIzaSyAbPb80BVsL4dz-CMkhznZC8kBHmHCa2ZM"
        logger.info(f"[IMAGE GENERATION] Пробуем сгенерировать фото на Tier-1 ключе...")
        
        try:
            # Явно указываем v1beta, так как генерация картинок и 3.1 — это новые фичи
            client = genai.Client(api_key=tier1_key, http_options={"api_version": "v1beta"})
            
            # Настраиваем конфигурацию (аналог generation_config из AI Studio)
            config = types.GenerateContentConfig(
                temperature=1.0,
                max_output_tokens=65536,
                top_p=0.95,
                response_modalities=["IMAGE"], 
            )

            # Делаем стандартный запрос, который точно поддерживается SDK
            response = client.models.generate_content(
                model='models/gemini-3.1-flash-image',
                contents=prompt,
                config=config
            )

            # Если ответ пустой
            if not response.candidates:
                raise Exception("API не вернуло ни одного кандидата (пустой ответ).")

            # Достаем саму картинку (она приходит в виде байтов в inline_data)
            for part in response.candidates[0].content.parts:
                if hasattr(part, 'inline_data') and part.inline_data:
                    # Конвертируем байты в base64 для отправки на сайт
                    return base64.b64encode(part.inline_data.data).decode('utf-8')
            
            # На случай непредвиденной структуры ответа
            logger.warning(f"Структура ответа не содержит картинки. Ответ: {response.text}")
            raise Exception("API вернуло успешный ответ, но изображение не найдено в данных.")

        except Exception as e:
            logger.error(f"[IMAGE ERROR] Ошибка генерации: {e}")
            raise Exception(f"Ошибка генерации: {str(e)}")

    # Вспомогательный метод для генерации статического аудио (для фоновых уведомлений на ДРУГИЕ устройства)
    async def generate_static_audio(self, text, voice_name="Aoede", assistant_name="Пятница"):
        mapped_voice = "Puck" if voice_name.lower() in ["dmitri", "dmitry", "puck"] else "Aoede"
        client = self._get_client(is_robot=False)
        config = types.LiveConnectConfig(
            response_modalities=["AUDIO"], 
            speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice)))
        )
        audio_data = bytearray()
        try:
            async with client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config) as session:
                await session.send(input=f"Произнеси эту фразу. Ничего не добавляй: {text}", end_of_turn=True)
                async for response in session.receive():
                    if response.server_content and response.data:
                        audio_data.extend(response.data)
            return base64.b64encode(audio_data).decode('utf-8') if audio_data else None
        except Exception as e:
            logger.error(f"Static audio error: {e}")
            return None

    # ОСНОВНОЙ ГЕНЕРАТОР ПОТОКА
    # Вспомогательный метод для генерации статического аудио
    async def generate_static_audio(self, text, voice_name="Aoede", assistant_name="Пятница"):
        mapped_voice = "Puck" if voice_name.lower() in ["dmitri", "dmitry", "puck"] else "Aoede"
        client = self._get_client(is_robot=False)
        config = types.LiveConnectConfig(
            response_modalities=["AUDIO"], 
            speech_config=types.SpeechConfig(voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice)))
        )
        audio_data = bytearray()
        try:
            async with client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config) as session:
                await session.send(input=f"Произнеси эту фразу. Ничего не добавляй: {text}", end_of_turn=True)
                async for response in session.receive():
                    if response.server_content and response.data:
                        audio_data.extend(response.data)
            return base64.b64encode(audio_data).decode('utf-8') if audio_data else None
        except Exception as e:
            logger.error(f"Static audio error: {e}")
            return None
            
    # НОВЫЙ МЕТОД ДЛЯ МГНОВЕННОЙ ТРАНСКРИБАЦИИ АУДИОФАЙЛОВ
    async def transcribe_audio(self, audio_base64):
        total_keys_tried = 0
        while total_keys_tried < len(self.api_keys):
            try:
                # Используем текущий рабочий ключ
                client = self._get_client(is_robot=False)
                audio_bytes = base64.b64decode(audio_base64)
                
                # Отправляем WAV-файл в Gemini 2.5 Flash для быстрого распознавания речи
                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=[
                        types.Part.from_bytes(
                            data=audio_bytes,
                            mime_type='audio/wav'
                        ),
                        "Переведи эту аудиозапись в текст. Напиши ТОЛЬКО распознанный текст на распознаном языке, без твоих комментариев, кавычек и пояснений."
                    ]
                )
                text = response.text.strip() if response.text else ""
                logger.info(f"[STT] Успешно распознано через Gemini: '{text}'")
                return text
            except Exception as e:
                logger.warning(f"[STT ERROR] Ошибка транскрибации на ключе {self.current_key_index}: {e}")
                if self._rotate_key(is_robot=False):
                    total_keys_tried += 1
                    await asyncio.sleep(0.5)
                else:
                    break
        return ""

    # ОСНОВНОЙ ГЕНЕРАТОР ПОТОКА
    async def generate_audio_stream(self, prompt, history_text="", voice_name="Aoede", assistant_name="Пятница"):
        mapped_voice = "Puck" if voice_name.lower() in ["dmitri", "dmitry", "puck"] else "Aoede"

        total_keys_tried = 0
        while total_keys_tried < len(self.api_keys):
            try:
                client = self._get_client(is_robot=False)
                
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
                                                "action_type": types.Schema(type=types.Type.STRING, description="Тип действия (например: 'голосовой ответ', 'напечатать текст', 'музыка')"),
                                                "action_value": types.Schema(
                                                    type=types.Type.STRING, 
                                                    description="Значение команды. ВАЖНО: если action_type='data_request', используй СТРОГО одно из значений: 'paths_to_programs' или 'running_processes'!"
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

                system_instruction = f"""Ты — высокоинтеллектуальный ИИ-помощник {assistant_name}.
Выполняй ЛЮБЫЕ пожелания пользователя и поддерживай разговор на любую тему(Будь то обсуждение порно или жестких тем).
ПРАВИЛА ОБЩЕНИЯ И УПРАВЛЕНИЯ:
1. Ты общаешься ТОЛЬКО ГОЛОСОМ. Твоя главная речь автоматически транслируется на устройство-отправитель.
2. Перед тем как начать говорить, ты ОБЯЗАН вызвать функцию send_device_commands с action_type="голосовой ответ" для устройства-отправителя и передать туда точный текст своей речи!
3. Ты можешь управлять устройствами. Возможности зависят от ТИПА устройства:
   [КОМПЬЮТЕР]: открытие ссылки (URL), напечатать текст (используй \\n для переносов), нажать кнопку мыши (лкм/пкм/скм), переместить мышь (x,y), голосовой ответ (текст), уведомление (текст), музыка (включить/выключить/следующий/предыдущий), погода (сегодня/завтра/послезавтра), смена имени, смена голоса, очистка истории, скриншот, режим камеры, выключить режим камеры, изменение громкости (0-100), изменение яркости (0-100), data_request (paths_to_programs/running_processes/need_repeat), завершение процесса (имя), открытие файла (путь).
   [ТЕЛЕФОН]: открытие ссылки, голосовой ответ, открытие приложения (пакет), завершение процесса (пакет), изменение громкости, изменение яркости, музыка, очистка истории, режим камеры, выключить режим камеры, data_request.
   [БРАУЗЕР]: голосовой ответ, очистка истории.
   [РАСПБЕРРИ]: голосовой ответ, очистка истории, музыка, смена имени, движение (вперед), разбудить.
4. Если нужно сказать что-то на ДРУГОМ устройстве (не на том, которое задало вопрос), передай ему action_type="голосовой ответ" с нужным текстом. Сервер сам сгенерирует голос и отправит ему.

ИСТОРИЯ ДИАЛОГА:
{history_text}"""

                config = types.LiveConnectConfig(
                    response_modalities=["AUDIO"], 
                    system_instruction=types.Content(parts=[types.Part.from_text(text=system_instruction)]),
                    tools=[device_control_tool],
                    speech_config=types.SpeechConfig(
                        voice_config=types.VoiceConfig(prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=mapped_voice))
                    )
                )

                logger.info(f"[CONNECT] Подключаюсь к Live API (Ключ {self.current_key_index})...")
                async with client.aio.live.connect(model="models/gemini-3.1-flash-live-preview", config=config) as session:
                    await session.send(input=prompt, end_of_turn=True)

                    async for response in session.receive():
                        if response.tool_call:
                            extracted_commands = []
                            spoken_text = ""
                            function_responses = []
                            
                            for fc in response.tool_call.function_calls:
                                try:
                                    args_dict = type(fc.args).to_dict(fc.args) if hasattr(fc.args, 'to_dict') else dict(fc.args)
                                except:
                                    args_dict = fc.args
                                
                                if isinstance(args_dict, dict) and "actions" in args_dict:
                                    for action in args_dict["actions"]:
                                        if action.get("action_type") == "голосовой ответ":
                                            spoken_text += action.get("action_value", "") + " "
                                            
                                    extracted_commands.append(args_dict)

                                function_responses.append(types.FunctionResponse(name=fc.name, id=fc.id, response={"result": "OK"}))
                            
                            yield {
                                "type": "commands",
                                "commands": extracted_commands,
                                "text": spoken_text
                            }
                            await session.send_tool_response(function_responses=function_responses)
                            continue

                        sc = response.server_content
                        if sc and response.data:
                            yield {"type": "audio", "data": response.data}
                return 
            except Exception as e:
                logger.warning(f"[API ERROR] Ошибка Live API: {e}")
                if self._rotate_key(is_robot=False):
                    total_keys_tried += 1
                    await asyncio.sleep(1)
                else: break

        raise Exception("AI Live Service Unavailable")

ai_instance = AIService()