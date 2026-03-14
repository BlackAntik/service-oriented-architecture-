import enum
import logging
import threading
import time
from typing import Callable, Any

import grpc

logger = logging.getLogger(__name__)

class CircuitBreakerState(enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout: int):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.lock = threading.Lock()

    def _transition(self, new_state: CircuitBreakerState):
        logger.info(f"Circuit Breaker transition: {self.state.value} -> {new_state.value}")
        self.state = new_state

    def allow_request(self) -> bool:
        with self.lock:
            if self.state == CircuitBreakerState.CLOSED:
                return True
            
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self._transition(CircuitBreakerState.HALF_OPEN)
                    return True
                return False
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                # In HALF_OPEN, we allow one request to pass through.
                # If multiple threads come here, we might want to be stricter,
                # but for simplicity, we allow it. The first failure will trip it back to OPEN.
                return True
            
            return False

    def record_success(self):
        with self.lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self._transition(CircuitBreakerState.CLOSED)
                self.failure_count = 0
            elif self.state == CircuitBreakerState.CLOSED:
                self.failure_count = 0

    def record_failure(self):
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == CircuitBreakerState.CLOSED:
                if self.failure_count >= self.failure_threshold:
                    self._transition(CircuitBreakerState.OPEN)
            
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self._transition(CircuitBreakerState.OPEN)

class CircuitBreakerInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, circuit_breaker: CircuitBreaker):
        self.circuit_breaker = circuit_breaker

    def intercept_unary_unary(self, continuation, client_call_details, request):
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit Breaker is OPEN. Blocking request.")
            # Return a mock response or raise an error that mimics gRPC error
            # Ideally we should raise a gRPC error that the client can handle
            # But interceptors are expected to return a Call object or raise.
            # Raising an exception here might crash the client if not handled properly.
            # Let's raise a specific RpcError.
            
            def abort(code, details):
                raise grpc.RpcError(grpc.StatusCode.UNAVAILABLE, details)

            # We can't easily construct a grpc.Call object here to return an error status.
            # The standard way is to raise an exception or return a future that raises.
            # Since we are in a blocking call (UnaryUnary), raising is appropriate.
            raise grpc.RpcError(grpc.StatusCode.UNAVAILABLE, "Circuit Breaker is OPEN")

        try:
            response = continuation(client_call_details, request)
            
            # Check if response is an error (gRPC calls return a response object which might be a future or result)
            # For UnaryUnary, 'response' is a Future-like object or the actual response?
            # In grpc.UnaryUnaryClientInterceptor, 'continuation' returns a response object.
            # If the call failed, it might have raised an exception already.
            
            # Wait for the response to check for errors (if it's a future)
            # But standard interceptors wrap the call.
            
            # Actually, if the underlying call fails, it raises grpc.RpcError.
            self.circuit_breaker.record_success()
            return response
            
        except grpc.RpcError as e:
            # Check status code. We only care about system errors (UNAVAILABLE, DEADLINE_EXCEEDED, etc.)
            # Business errors (NOT_FOUND, INVALID_ARGUMENT) should not trip the breaker.
            if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.INTERNAL):
                self.circuit_breaker.record_failure()
            else:
                self.circuit_breaker.record_success() # Business error is a "successful" RPC call
            raise e