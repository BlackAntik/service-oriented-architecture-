import unittest
from unittest.mock import MagicMock, patch
import time
import logging
import os
import sys
import grpc

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
app_dir = os.path.join(project_root, "homework3", "booking_service", "app")
gen_dir = os.path.join(app_dir, "gen")
sys.path.append(app_dir)
sys.path.append(gen_dir)

os.environ["DATABASE_URL"] = "postgresql://booking:booking@localhost:5433/bookingdb"
os.environ["FLIGHT_GRPC_ADDR"] = "localhost:50051"
os.environ["FLIGHT_SERVICE_API_KEY"] = "secret-api-key-123"
os.environ["CIRCUIT_BREAKER_FAILURE_THRESHOLD"] = "3"
os.environ["CIRCUIT_BREAKER_RECOVERY_TIMEOUT"] = "1"

from circuit_breaker import CircuitBreaker, CircuitBreakerState, CircuitBreakerInterceptor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_circuit_breaker")

class TestCircuitBreaker(unittest.TestCase):
    def setUp(self):
        self.cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)

    def test_initial_state(self):
        self.assertEqual(self.cb.state, CircuitBreakerState.CLOSED)

    def test_state_transitions(self):
        self.cb.record_failure()
        self.cb.record_failure()
        self.assertEqual(self.cb.state, CircuitBreakerState.CLOSED)
        self.cb.record_failure()
        self.assertEqual(self.cb.state, CircuitBreakerState.OPEN)
        logger.info("Transitioned to OPEN")

        time.sleep(1.1)
        self.assertTrue(self.cb.allow_request())
        self.assertEqual(self.cb.state, CircuitBreakerState.HALF_OPEN)
        logger.info("Transitioned to HALF_OPEN")

        self.cb.record_failure()
        self.assertEqual(self.cb.state, CircuitBreakerState.OPEN)
        logger.info("Transitioned back to OPEN")

        time.sleep(1.1)
        self.assertTrue(self.cb.allow_request())
        self.cb.record_success()
        self.assertEqual(self.cb.state, CircuitBreakerState.CLOSED)
        logger.info("Transitioned to CLOSED")

class TestCircuitBreakerInterceptor(unittest.TestCase):
    def setUp(self):
        self.cb = MagicMock()
        self.interceptor = CircuitBreakerInterceptor(self.cb)

    def test_intercept_blocked(self):
        self.cb.allow_request.return_value = False
        
        with self.assertRaises(grpc.RpcError) as cm:
            self.interceptor.intercept_unary_unary(None, None, None)
        
        self.assertEqual(cm.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertIn("Circuit Breaker is OPEN", str(cm.exception))
        logger.info("Interceptor correctly blocked request when CB is OPEN")

    def test_intercept_success(self):
        self.cb.allow_request.return_value = True
        
        mock_continuation = MagicMock()
        mock_response = MagicMock()
        mock_continuation.return_value = mock_response
        
        result = self.interceptor.intercept_unary_unary(mock_continuation, None, None)
        
        self.assertEqual(result, mock_response)
        self.cb.record_success.assert_called_once()
        logger.info("Interceptor recorded success on successful call")

    def test_intercept_failure(self):
        self.cb.allow_request.return_value = True
        
        mock_continuation = MagicMock()
        
        error = grpc.RpcError()
        error.code = lambda: grpc.StatusCode.UNAVAILABLE
        mock_continuation.side_effect = error
        
        with self.assertRaises(grpc.RpcError):
            self.interceptor.intercept_unary_unary(mock_continuation, None, None)
            
        self.cb.record_failure.assert_called_once()
        logger.info("Interceptor recorded failure on UNAVAILABLE error")

    def test_intercept_business_error(self):
        self.cb.allow_request.return_value = True
        
        mock_continuation = MagicMock()
        
        error = grpc.RpcError()
        error.code = lambda: grpc.StatusCode.NOT_FOUND
        mock_continuation.side_effect = error
        
        with self.assertRaises(grpc.RpcError):
            self.interceptor.intercept_unary_unary(mock_continuation, None, None)
            
        self.cb.record_success.assert_called_once()
        logger.info("Interceptor recorded success on business error (NOT_FOUND)")

if __name__ == "__main__":
    unittest.main()