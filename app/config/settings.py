import os
import logging

# Настройки логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Секреты
JWT_SECRET = 'friday2avelgar'
GOOGLE_API_KEY = "AIzaSyAbPb80BVsL4dz-CMkhznZC8kBHmHCa2ZM" # Лучше брать из os.environ

# Настройки БД
DB_CONFIG = {
    'user': 'friday_user',
    'password': 'testzxC13!',
    'host': '127.0.0.1',
    'database': 'friday_db',
}

# Настройки Email
SMTP_CONFIG = {
    'host': 'smtp.resend.com',
    'port': 465,
    'user': 'resend',  # В Resend имя пользователя всегда строго слово "resend"
    'password': 're_P5sD4BjY_59MtvZoou6tB2HDnYpXK5pva'  # Твой API-ключ
}