# -*- coding: utf-8 -*-
import os
import logging

# Настройки логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Читаем секреты из окружения (systemd передаст их сюда из .env)
JWT_SECRET = os.environ.get('JWT_SECRET', '')
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY', '')

# Настройки БД
DB_CONFIG = {
    'user': os.environ.get('DB_USER', 'friday_user'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'database': os.environ.get('DB_NAME', 'friday_db'),
}

# Настройки Email
SMTP_CONFIG = {
    'host': os.environ.get('SMTP_HOST', 'smtp.resend.com'),
    'port': int(os.environ.get('SMTP_PORT', 465)),
    'user': os.environ.get('SMTP_USER', 'resend'),
    'password': os.environ.get('SMTP_PASSWORD', ''),
}