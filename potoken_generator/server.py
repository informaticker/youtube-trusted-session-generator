import logging
import time
import asyncio
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from json import dumps
from typing import Optional, Tuple, Dict
import threading

from potoken_generator.extractor import PotokenExtractor, TokenInfo

logger = logging.getLogger('server')


class PotokenRequestHandler(BaseHTTPRequestHandler):
    potoken_server = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == '/token':
            self._handle_get_token()
        elif self.path == '/':
            self._send_redirect('/token')
        else:
            self._send_not_found()

    def do_POST(self):
        if self.path == '/update':
            self._handle_update_token()
        else:
            self._send_not_found()

    def _handle_get_token(self):
        token = self.potoken_server._potoken_extractor.get()
        if token is None:
            self._send_response(503, 'Service Unavailable',
                               'Token has not yet been generated, try again later.',
                               content_type='text/plain')
        else:
            self._send_response(200, 'OK', token.to_json(),
                               content_type='application/json')

    def _handle_update_token(self):
        extractor = self.potoken_server._potoken_extractor
        current_token = extractor.get()
        current_updated_time = current_token.updated if current_token else 0

        update_event = threading.Event()
        update_result = {"new_token": None}

        async def token_updated_callback(token: TokenInfo):
            if token.updated > current_updated_time:
                update_result["new_token"] = token
                update_event.set()

        asyncio.run_coroutine_threadsafe(
            extractor.register_token_update_callback(token_updated_callback),
            extractor._loop
        )

        accepted = extractor.request_update()

        if not accepted:
            asyncio.run_coroutine_threadsafe(
                extractor.unregister_token_update_callback(token_updated_callback),
                extractor._loop
            )
            self._send_response(409, 'Conflict',
                               'Update already in progress or requested.',
                               content_type='text/plain')
            return

        if update_event.wait(60):
            new_token = update_result["new_token"]
            asyncio.run_coroutine_threadsafe(
                extractor.unregister_token_update_callback(token_updated_callback),
                extractor._loop
            )
            self._send_response(200, 'OK',
                              f'{new_token.to_json()}',
                              content_type='application/json')
        else:
            asyncio.run_coroutine_threadsafe(
                extractor.unregister_token_update_callback(token_updated_callback),
                extractor._loop
            )
            self._send_response(504, 'Gateway Timeout',
                              'Timed out waiting for token update.',
                              content_type='text/plain')

    def _send_not_found(self):
        self._send_response(404, 'Not Found', 'Not Found',
                           content_type='text/plain')

    def _send_redirect(self, location):
        self.send_response(302)
        self.send_header('Location', location)
        self.end_headers()

    def _send_response(self, status_code, status_message, content, content_type='text/plain'):
        self.send_response(status_code, status_message)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(content.encode('utf-8'))))
        self.end_headers()
        self.wfile.write(content.encode('utf-8'))

    def log_message(self, format, *args):
        pass


class PotokenServer:
    def __init__(self, potoken_extractor: PotokenExtractor, port: int = 8080, bind_address: str = '0.0.0.0') -> None:
        self.port = port
        self.bind_address = bind_address
        self._potoken_extractor = potoken_extractor
        self._httpd: Optional[ThreadingHTTPServer] = None

    def run(self) -> None:
        logger.info(f'Starting web-server at {self.bind_address}:{self.port}')
        PotokenRequestHandler.potoken_server = self
        self._httpd = ThreadingHTTPServer((self.bind_address, self.port), PotokenRequestHandler)
        with self._httpd:
            self._httpd.serve_forever()

    def stop(self) -> None:
        if self._httpd is None:
            return
        self._httpd.shutdown()
