import os
import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

logger = logging.getLogger("HTTP_Server")

from app.http_server.routes_get import do_GET
from app.http_server.routes_post import do_POST

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    pass

class HTTPRequestHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    
    do_GET = do_GET
    do_POST = do_POST

    def send_json(self, status_code, data):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_sse(self, data):
        try:
            msg = f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode('utf-8')
            chunk_header = f"{len(msg):X}\r\n".encode('utf-8')
            self.wfile.write(chunk_header)
            self.wfile.write(msg)
            self.wfile.write(b"\r\n")
            self.wfile.flush()
        except Exception as e:
            logger.error(f"Ошибка при отправке SSE чанка: {e}")

    def serve_file(self, filename, content_type, download_name=None):
        try:
            file_path = os.path.join('/opt/friday', filename)
            with open(file_path, 'rb') as f:
                content = f.read()
                self.send_response(200)
                self.send_header('Content-type', content_type)
                self.send_header('Content-Length', str(len(content)))
                if download_name: self.send_header('Content-Disposition', f'attachment; filename="{download_name}"')
                self.end_headers()
                self.wfile.write(content)
        except FileNotFoundError:
            self.send_error(404, "File Not Found")

    def redirect(self, url):
        self.send_response(302)
        self.send_header('Location', url)
        self.end_headers()

    def handle_one_request(self):
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if not self.raw_requestline:
                self.close_connection = True
                return

            try: self.requestline = self.raw_requestline.decode('utf-8', errors='ignore')[:100]
            except: self.requestline = str(self.raw_requestline[:100])

            if not self.raw_requestline.startswith((b'GET', b'POST', b'PUT', b'DELETE', b'HEAD', b'OPTIONS')):
                try:
                    data = json.loads(self.raw_requestline.decode('utf-8').strip())
                    self.requestline = "POST /raw_json HTTP/1.1"
                    self.command = "POST"
                    self.path = "/raw_json"
                    self.headers = {}
                    self.raw_data = data
                    self.do_POST()
                    return
                except:
                    self.send_error(400, "Bad Request")
                    return

            if not self.parse_request(): return
            method = 'do_' + self.command
            if hasattr(self, method): getattr(self, method)()
            else: self.send_error(501, "Unsupported method")
                
        except Exception as e:
            logger.error(f"Error handling request: {e}")
            self.close_connection = True