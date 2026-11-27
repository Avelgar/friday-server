# -*- coding: utf-8 -*-
import logging
from app.services.ai.key_pool import GEMINI_KEYS, GeminiKeyPool, create_genai_client, create_live_client
from app.services.ai import live_voice, router as ai_router, antigravity as ag

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class AIService:
    def __init__(self):
        self.api_keys = GEMINI_KEYS
        self.current_key_index = 0
        self.key_pool = GeminiKeyPool(self.api_keys)

    def _get_client(self, key_index=None, timeout=180000):
        idx = key_index if key_index is not None else self.current_key_index
        return create_genai_client(self.api_keys, idx, timeout=timeout)

    def _get_live_client(self):
        return create_live_client(self.api_keys)

    # --- 1. ГОЛОС / TTS (Gemini Live) ---
    async def generate_static_audio(self, text, voice_name="Aoede", assistant_name="Пятница"):
        return await live_voice.generate_static_audio(self.api_keys, text, voice_name, assistant_name)

    async def generate_static_audio_stream(self, text, voice_name="Aoede", assistant_name="Пятница"):
        async for chunk in live_voice.generate_static_audio_stream(self.api_keys, text, voice_name, assistant_name):
            yield chunk

    async def generate_audio_stream(self, prompt_text, system_instruction, allowed_actions, audio_bytes=None, image_bytes=None, formatted_history=None, voice_name="Aoede", assistant_name="Пятница", media_queue=None):
        async for item in live_voice.generate_audio_stream(
            self.api_keys, prompt_text, system_instruction, allowed_actions,
            audio_bytes=audio_bytes, image_bytes=image_bytes, formatted_history=formatted_history,
            voice_name=voice_name, assistant_name=assistant_name, media_queue=media_queue
        ):
            yield item

    async def generate_audio_stream_realtime(self, prompt_text, system_instruction, allowed_actions, media_queue, formatted_history=None, voice_name="Aoede", assistant_name="Пятница"):
        async for item in live_voice.generate_audio_stream_realtime(
            self.api_keys, prompt_text, system_instruction, allowed_actions, media_queue,
            formatted_history=formatted_history, voice_name=voice_name, assistant_name=assistant_name
        ):
            yield item

    # --- 2. DIRECT И ПЛАНИРОВЩИК ---
    async def run_direct(
        self,
        prompt_text: str,
        recent_history: list = None,
        project_tree: str = None,
        file_manifest: list = None,
        status_callback=None,
        chunk_callback=None,
        assigned_key: int = None,
        per_request_key: bool = True,
    ):
        return await ai_router.run_direct(
            self.key_pool,
            self.api_keys,
            prompt_text,
            recent_history=recent_history,
            project_tree=project_tree,
            file_manifest=file_manifest,
            status_callback=status_callback,
            chunk_callback=chunk_callback,
            assigned_key=assigned_key,
            per_request_key=per_request_key,
        )

    async def run_planner(
        self,
        task_prompt: str,
        recent_history: list = None,
        project_tree: str = None,
        file_manifest: list = None,
        selected_files: list = None,
        status_callback=None,
        assigned_key: int = None,
        per_request_key: bool = True,
    ):
        return await ai_router.run_planner(
            self.key_pool,
            self.api_keys,
            task_prompt,
            recent_history=recent_history,
            project_tree=project_tree,
            file_manifest=file_manifest,
            selected_files=selected_files,
            status_callback=status_callback,
            assigned_key=assigned_key,
            per_request_key=per_request_key,
        )

    # --- 3. ПЕСОЧНИЦА ANTIGRAVITY ---
    async def run_antigravity_agent(
        self,
        prompt_text: str,
        status_callback=None,
        assigned_key: int = None,
        chat_uid: str = None,
        recent_history: list = None,
        project_tree: str = None,
        file_manifest: list = None,
        selected_files: list = None,
        line_ranges: str = "",
        need_network: bool = False,
        max_total_tokens: int = 85_000,
        per_request_key: bool = True,
        **kwargs,
    ):
        return await ag.run_antigravity_agent(
            self.key_pool,
            self.api_keys,
            prompt_text,
            status_callback=status_callback,
            assigned_key=assigned_key,
            chat_uid=chat_uid,
            recent_history=recent_history,
            project_tree=project_tree,
            file_manifest=file_manifest,
            selected_files=selected_files,
            line_ranges=line_ranges,
            need_network=need_network,
            max_total_tokens=max_total_tokens,
            per_request_key=per_request_key,
        )

    async def list_environment_files(self, env_id: str, key_index: int = None):
        return await ag.list_environment_files(self.api_keys, env_id, key_index=key_index)

    async def download_environment_file(self, env_id: str, remote_path: str, chat_uid: str, key_index: int = None):
        return await ag.download_environment_file(self.api_keys, env_id, remote_path, chat_uid, key_index=key_index)

    async def upload_file_to_environment(self, env_id: str, local_server_path: str, remote_path: str, key_index: int = None):
        return await ag.upload_file_to_environment(self.api_keys, env_id, local_server_path, remote_path, key_index=key_index)


ai_instance = AIService()