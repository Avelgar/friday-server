import smtplib
from email.mime.text import MIMEText
from email.header import Header
# Представим, что SMTP_CONFIG импортируется отсюда
from app.config.settings import SMTP_CONFIG
import logging

logger = logging.getLogger(__name__)

def send_email(email, subject, body):
    logger.info(f"--- Отправка письма на {email} ---")
    try:
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Subject'] = Header(subject, 'utf-8')
        
        # ИСПРАВЛЕНО: Указываем твою реальную почту и красивое имя отправителя
        msg['From'] = 'Friday Assistant <noreply@friday-assistant.ru>'
        msg['To'] = email
        
        with smtplib.SMTP_SSL(SMTP_CONFIG['host'], SMTP_CONFIG['port'], timeout=10) as server:
            server.login(SMTP_CONFIG['user'], SMTP_CONFIG['password'])
            server.send_message(msg)
            
        logger.info("Письмо успешно отправлено")
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки письма: {e}")
        return False