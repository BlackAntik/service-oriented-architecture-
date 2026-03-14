import enum
import logging
import threading
import time

import grpc
from grpc import StatusCode
from grpc._channel import _InactiveRpcError, _RPCState

logger = logging.getLogger(__name__)


class CircuitBreakerOpenError(grpc.RpcError):
    
    def __init__(self, message: str):
        self._message = message
        self._code = StatusCode.UNAVAILABLE
        self._details = message
    
    def code(self):
        return self._code
    
    def details(self):
        return self._details
    
    def __str__(self):
        return self._message

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
            raise CircuitBreakerOpenError("Circuit Breaker is OPEN")

        try:
            response = continuation(client_call_details, request)

            self.circuit_breaker.record_success()
            return response

        except grpc.RpcError as e:
            if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.INTERNAL):
                self.circuit_breaker.record_failure()
            else:
                self.circuit_breaker.record_success()
            raise e