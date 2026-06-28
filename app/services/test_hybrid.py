import sys
# Указываем питону, где лежит корень проекта
sys.path.append('/opt/friday')

import asyncio
import base64
import wave
import os
import json # Добавили для красивого вывода команд

# Теперь импорт сработает!
from app.services.ai_service import ai_instance

async def main():
    print("⏳ Запускаю тестовый звонок в Gemini Live API с проверкой Function Calling...")
    
    # 1. Изменим промпт так, чтобы спровоцировать вызов функции И аудио-ответ
    user_prompt = "Пятница открой википедию"
    prompt = f"""[КОНТЕКСТ ТЕКУЩЕГО ЗАПРОСА]
Время: 20:19
Устройство отправителя: Компьютер Кирилла (Тип: Телефон)
Доступные устройства в сети: нет

Сообщение пользователя:
Открой на телефоне шахматы и скажи какая погода в Москве сегодня
"""
    fake_history = "Пользователь: Привет!\nАссистент: Здравствуйте, сэр. Я в сети."
    
    try:
        # 2. Делаем запрос к обновленному методу (теперь возвращает 3 значения!)
        audio_b64, text_reply, commands = await ai_instance.generate_audio_response(
            prompt=prompt,
            history_text=fake_history,
            voice_name="Aoede",
            assistant_name="Пятница"
        )
        
        print(f"\n🤖 Текст ответа: {text_reply}\n")
        
        # --- НОВОЕ: ВЫВОД КОМАНД ---
        if commands:
            print("⚙️ Gemini решила отправить следующие команды на устройства:")
            # Переводим словарь в красивый JSON с отступами
            for cmd in commands:
                # В Gemini Live API возвращаемые аргументы могут быть объектом protobuf, 
                # мы преобразуем их в dict для надежного вывода
                cmd_dict = type(cmd).to_dict(cmd) if hasattr(cmd, 'to_dict') else dict(cmd)
                print(json.dumps(cmd_dict, indent=2, ensure_ascii=False))
        else:
            print("⚙️ Gemini не стала вызывать функции управления устройствами в этот раз.")
            
        print("-" * 40)
        
        # 3. Декодируем Base64 обратно в сырые аудио-байты (PCM)
        if audio_b64:
            pcm_bytes = base64.b64decode(audio_b64)
            
            # 4. Сохраняем PCM байты в нормальный WAV файл
            filename = "test_response.wav"
            with wave.open(filename, "wb") as f:
                f.setnchannels(1)          # Моно
                f.setsampwidth(2)          # 16-bit
                f.setframerate(24000)      # Gemini Live API возвращает звук 24kHz
                f.writeframes(pcm_bytes)
                
            print(f"\n✅ Готово! Аудио сохранено в файл: {os.path.abspath(filename)}")
        else:
            print("\n⚠️ ИИ не сгенерировал голос (возможно, выполнил команды молча).")

    except Exception as e:
        print(f"❌ Произошла ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(main())