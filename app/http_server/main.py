import sys
sys.path.append('/opt/friday')

import threading
import logging
from app.http_server.server_core import ThreadingHTTPServer, HTTPRequestHandler
from app.http_server.tasks import clean_expired_tokens

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("HTTP_Server")

def run():
    threading.Thread(target=clean_expired_tokens, daemon=True).start()
    server = ThreadingHTTPServer(('0.0.0.0', 25550), HTTPRequestHandler)
    logger.info("HTTPS сервер запущен на порту 25550 (Многопоточный, Рефакторинг)")
    server.serve_forever()

if __name__ == '__main__':
    run()