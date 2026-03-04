import time
import uuid
import json
import logging
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Callable, Any

logger = logging.getLogger("api_logger")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(handler)

class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        request_id = str(uuid.uuid4())
        start_time = time.time()
        
        request_body = None
        if request.method in ["POST", "PUT", "DELETE"]:
            try:
                body_bytes = await request.body()
                if body_bytes:
                    try:
                        request_body = json.loads(body_bytes)
                        self._mask_sensitive_data(request_body)
                    except json.JSONDecodeError:
                        request_body = body_bytes.decode("utf-8", errors="replace")
                
                async def receive():
                    return {"type": "http.request", "body": body_bytes}
                request._receive = receive
                
            except Exception as e:
                logger.error(f"Error reading request body: {e}")

        response = await call_next(request)
        
        duration_ms = (time.time() - start_time) * 1000
        
        user_id = request.headers.get("X-User-Id")

        log_data = {
            "request_id": request_id,
            "method": request.method,
            "endpoint": request.url.path,
            "status_code": response.status_code,
            "duration_ms": round(duration_ms, 2),
            "user_id": user_id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
        
        if request_body:
            log_data["body"] = request_body

        logger.info(json.dumps(log_data, ensure_ascii=False))
        
        response.headers["X-Request-Id"] = request_id
        return response

    def _mask_sensitive_data(self, data: Any):
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    self._mask_sensitive_data(value)
                elif key.lower() in ["password", "token", "secret", "authorization"]:
                    data[key] = "***"
        elif isinstance(data, list):
            for item in data:
                self._mask_sensitive_data(item)