# -*- coding: utf-8 -*-
import base64
import asyncio
import logging
import time
from google.genai import types
from .key_pool import create_live_client

logger = logging.getLogger("AI_Service")


async def generate_static_audio(api_keys, text, voice_name="Aoede", assistant_name="Пятница"):
    voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
    valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
    mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"

    client = create_live_client(api_keys)
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
    except Exception:
        pass
    finally:
        if session:
            try:
                await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
            except Exception:
                pass
    return base64.b64encode(audio_data).decode('utf-8') if audio_data else None


async def generate_static_audio_stream(api_keys, text, voice_name="Aoede", assistant_name="Пятница"):
    voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
    valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
    mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"

    client = create_live_client(api_keys)
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
            try:
                await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
            except Exception:
                pass


def build_device_control_tool(allowed_actions):
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


async def generate_audio_stream(api_keys, prompt_text, system_instruction, allowed_actions, audio_bytes=None, image_bytes=None, formatted_history=None, voice_name="Aoede", assistant_name="Пятница", media_queue=None):
    voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
    valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
    mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"
    has_yielded_data = False

    try:
        client = create_live_client(api_keys)
        device_control_tool = build_device_control_tool(allowed_actions)

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
                try:
                    await session.send_client_content(turns=formatted_history[-10:], turn_complete=True)
                except Exception:
                    pass

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
            waiting_for_tool_continuation = False

            while True:
                try:
                    response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=30.0)
                except StopAsyncIteration:
                    if waiting_for_tool_continuation:
                        waiting_for_tool_continuation = False
                        receive_iterator = session.receive().__aiter__()
                        continue
                    break
                except (asyncio.TimeoutError, TimeoutError):
                    if has_yielded_data:
                        break
                    raise

                if response.tool_call:
                    extracted_commands = []
                    function_responses = []
                    for fc in response.tool_call.function_calls:
                        args_dict = type(fc.args).to_dict(fc.args) if hasattr(fc.args, 'to_dict') else dict(fc.args)
                        if isinstance(args_dict, dict) and "actions" in args_dict:
                            extracted_commands.append(args_dict)
                        call_id = getattr(fc, 'id', None) or f"call_{int(time.time()*1000)}"
                        function_responses.append(types.FunctionResponse(
                            name=fc.name,
                            id=call_id,
                            response={"status": "ok", "result": "Action executed successfully. Continue speaking to the user now."}
                        ))

                    if extracted_commands:
                        has_yielded_data = True
                        yield {"type": "commands", "commands": extracted_commands}
                    try:
                        await session.send_tool_response(function_responses=function_responses)
                        waiting_for_tool_continuation = True
                    except Exception as tre:
                        logger.error(f"[TOOL RESPONSE SEND ERROR] {tre}")

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
                    is_idle = True
                    if sc.interaction_status is not None and sc.interaction_status != types.InteractionStatus.INTERACTION_STATUS_UNSPECIFIED:
                        is_idle = (sc.interaction_status == types.InteractionStatus.IDLE)
                    else:
                        is_idle = bool(sc.turn_complete)

                    if is_idle:
                        if waiting_for_tool_continuation:
                            waiting_for_tool_continuation = False
                            receive_iterator = session.receive().__aiter__()
                            continue
                        break
        except (asyncio.TimeoutError, TimeoutError):
            if has_yielded_data:
                return
        except StopAsyncIteration:
            pass
        finally:
            if sender_task:
                sender_task.cancel()
                try:
                    await sender_task
                except Exception:
                    pass
            if session:
                try:
                    await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                except Exception:
                    pass
        return
    except Exception as e:
        logger.error(f"[API ERROR] Ошибка Live API: {e}")
        if has_yielded_data:
            return
        raise Exception("AI Live Service Unavailable")


async def generate_audio_stream_realtime(api_keys, prompt_text, system_instruction, allowed_actions, media_queue, formatted_history=None, voice_name="Aoede", assistant_name="Пятница"):
    voice_clean = str(voice_name).strip().capitalize() if voice_name else "Aoede"
    valid_voices = ["Aoede", "Puck", "Kore", "Charon", "Zephyr", "Fenrir"]
    mapped_voice = voice_clean if voice_clean in valid_voices else "Aoede"

    session_audio_cache = bytearray()
    last_video_frame = None
    has_reached_stream_end = False
    has_yielded_data = False

    try:
        client = create_live_client(api_keys)
        device_control_tool = build_device_control_tool(allowed_actions)

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
                try:
                    await session.send_client_content(turns=formatted_history[-10:], turn_complete=True)
                except Exception:
                    pass

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
            waiting_for_tool_continuation = False

            while True:
                try:
                    response = await asyncio.wait_for(receive_iterator.__anext__(), timeout=30.0)
                except StopAsyncIteration:
                    if waiting_for_tool_continuation:
                        waiting_for_tool_continuation = False
                        receive_iterator = session.receive().__aiter__()
                        continue
                    break
                except (asyncio.TimeoutError, TimeoutError):
                    if has_yielded_data:
                        break
                    raise

                if response.tool_call:
                    extracted_commands = []
                    function_responses = []
                    for fc in response.tool_call.function_calls:
                        args_dict = type(fc.args).to_dict(fc.args) if hasattr(fc.args, 'to_dict') else dict(fc.args)
                        if isinstance(args_dict, dict) and "actions" in args_dict:
                            extracted_commands.append(args_dict)
                        call_id = getattr(fc, 'id', None) or f"call_{int(time.time()*1000)}"
                        function_responses.append(types.FunctionResponse(
                            name=fc.name,
                            id=call_id,
                            response={"status": "ok", "result": "Action executed successfully. Continue speaking to the user now."}
                        ))
                    if extracted_commands:
                        has_yielded_data = True
                        yield {"type": "commands", "commands": extracted_commands}
                    try:
                        await session.send_tool_response(function_responses=function_responses)
                        waiting_for_tool_continuation = True
                    except Exception as tre:
                        logger.error(f"[REALTIME TOOL RESPONSE ERROR] {tre}")

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
                    is_idle = True
                    if sc.interaction_status is not None and sc.interaction_status != types.InteractionStatus.INTERACTION_STATUS_UNSPECIFIED:
                        is_idle = (sc.interaction_status == types.InteractionStatus.IDLE)
                    else:
                        is_idle = bool(sc.turn_complete)

                    if is_idle:
                        if waiting_for_tool_continuation:
                            waiting_for_tool_continuation = False
                            receive_iterator = session.receive().__aiter__()
                            continue
                        break
        except (asyncio.TimeoutError, TimeoutError):
            if has_yielded_data:
                return
        except StopAsyncIteration:
            pass
        except Exception as loop_e:
            logger.error(f"[REALTIME LIVE LOOP ERROR] {loop_e}", exc_info=True)
        finally:
            if sender_task:
                sender_task.cancel()
            if session:
                try:
                    await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=3.0)
                except Exception:
                    pass
        return
    except Exception:
        if has_yielded_data:
            return
        raise Exception("AI Live Service Unavailable")
