# -*- coding: utf-8 -*-
import asyncio
import sys
import os
import wave

sys.path.append('/opt/friday')

from app.services.ai_service import AIService

async def main():
    if len(sys.argv) < 2:
        print("Использование: python3 test_hybrid.py <путь_к_wav_файлу>")
        return

    wav_path = sys.argv[1]
    
    if not os.path.exists(wav_path):
        print(f"❌ Файл {wav_path} не найден!")
        return

    with open(wav_path, "rb") as f:
        audio_bytes = f.read()
        
    print(f"✅ Загружено {len(audio_bytes)} байт из {wav_path}")
    
    ai = AIService()
    prompt = """[СИСТЕМНЫЕ ДАННЫЕ]
Устройство отправителя: Компьютер (Тип: компьютер).
Доступные устройства: нет.

[ЗАПРОС ПОЛЬЗОВАТЕЛЯ]:
"""
    
    print("🚀 Запускаем генератор Gemini Live API...\n")
    print("-" * 50)
    
    response_pcm = bytearray()

    try:
        async for chunk in ai.generate_audio_stream(
            prompt_text=prompt,
            audio_bytes=audio_bytes,
            image_bytes=None,
            history_text="Это тестовый запуск.",
            voice_name="Aoede",
            assistant_name="Пятница"
        ):
            if chunk["type"] == "user_text":
                print(f"👤 ТРАНСКРИПЦИЯ ЮЗЕРА: {chunk['text']}")
            elif chunk["type"] == "bot_text":
                print(f"🤖 ТЕКСТ БОТА: {chunk['text']}")
            elif chunk["type"] == "commands":
                print(f"🛠 КОМАНДЫ БОТА: {chunk['commands']}")
            elif chunk["type"] == "audio":
                print(f"🎵 ПРИЛЕТЕЛ АУДИО-ЧАНК: {len(chunk['data'])} байт")
                response_pcm.extend(chunk['data'])
                
        print("\n✅ СЕССИЯ УСПЕШНО ЗАВЕРШЕНА")

        # СОХРАНЯЕМ ОТВЕТ ГУГЛА В ФАЙЛ
        if response_pcm:
            out_wav = "/tmp/Friday_Response.wav"
            with wave.open(out_wav, "wb") as wf:
                wf.setnchannels(1)
                wf.setsampwidth(2) # 16-bit
                wf.setframerate(24000) # Gemini всегда возвращает 24kHz
                wf.writeframes(response_pcm)
            print(f"💾 Аудио-ответ сохранен в файл: {out_wav}")
        else:
            print("⚠️ Бот не прислал ни одного байта аудио!")

    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")

if __name__ == "__main__":
    asyncio.run(main())