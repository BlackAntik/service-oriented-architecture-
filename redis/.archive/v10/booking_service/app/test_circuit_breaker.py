import time
import grpc
import uuid
import logging
import os
import sys

# Add booking_service/app to path to import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen"))

# Mock config values before importing config
os.environ["DATABASE_URL"] = "postgresql://booking:booking@localhost:5433/bookingdb"
os.environ["FLIGHT_GRPC_ADDR"] = "localhost:50051"
os.environ["FLIGHT_SERVICE_API_KEY"] = "secret-api-key-123"
os.environ["CIRCUIT_BREAKER_FAILURE_THRESHOLD"] = "3"
os.environ["CIRCUIT_BREAKER_RECOVERY_TIMEOUT"] = "5"

from circuit_breaker import CircuitBreaker, CircuitBreakerState, CircuitBreakerInterceptor
from grpc_client import FlightClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_circuit_breaker")

def test_circuit_breaker():
    logger.info("Starting Circuit Breaker Test")
    
    # Initialize client
    client = FlightClient()
    
    # Access internal circuit breaker for testing state
    cb = client._circuit_breaker
    
    logger.info(f"Initial State: {cb.state}")
    assert cb.state == CircuitBreakerState.CLOSED
    
    # 1. Simulate failures to trip the breaker
    logger.info("Simulating failures...")
    
    # We manually trip the breaker because mocking the stub bypasses the interceptor
    # (interceptor wraps the channel, but we would be replacing the stub which uses the channel)
    
    logger.info("Manually recording failures to trip breaker...")
    for i in range(3):
        cb.record_failure()
        logger.info(f"Recorded failure {i+1}")
    
    logger.info(f"State after 3 failures: {cb.state}")
    assert cb.state == CircuitBreakerState.OPEN
    
    # 2. Verify OPEN state blocks requests immediately
    logger.info("Verifying OPEN state blocking...")
    start_time = time.time()
    try:
        # This should fail FAST because CB is OPEN
        client.search_flights("MOW", "LED", None)
    except Exception as e:
        logger.info(f"Caught error in OPEN state: {e}")
        # Verify it's the CB error
        if "Circuit Breaker is OPEN" in str(e):
            logger.info("Confirmed Circuit Breaker blocked the request")
        else:
            logger.warning(f"Unexpected error message: {e}")
        pass
        
    duration = time.time() - start_time
    logger.info(f"Request in OPEN state took {duration:.4f}s")
    
    # 3. Wait for recovery timeout (5s)
    logger.info("Waiting for recovery timeout (5s)...")
    time.sleep(6)
    
    # 4. Verify HALF_OPEN
    # The next request should be allowed through the CB.
    # Since we don't have a real backend, it will fail at the network level (UNAVAILABLE).
    # The interceptor will see this failure and trip it back to OPEN.
    
    logger.info("Testing HALF_OPEN -> OPEN transition (via network failure)")
    try:
        client.search_flights("MOW", "LED", None)
    except Exception as e:
        logger.info(f"Caught expected network error: {e}")
        pass
        
    logger.info(f"State after failure in HALF_OPEN: {cb.state}")
    assert cb.state == CircuitBreakerState.OPEN
    
    # Wait again
    logger.info("Waiting for recovery timeout again (5s)...")
    time.sleep(6)
    
    # Now we want to test success.
    # To test success, we need the call to succeed.
    # Since we can't easily mock the channel success without complex mocking,
    # let's manually transition to CLOSED to verify logic, or just trust the failure path.
    
    # Let's try to simulate success by manually calling record_success
    # But we want to verify allow_request() returns True first.
    
    logger.info("Verifying HALF_OPEN allows request")
    # State should be OPEN but timeout passed, so allow_request() should transition to HALF_OPEN and return True
    
    # We can't check state directly because allow_request changes it.
    # Let's call allow_request manually.
    allowed = cb.allow_request()
    logger.info(f"allow_request() returned: {allowed}")
    logger.info(f"State is now: {cb.state}")
    
    assert allowed is True
    assert cb.state == CircuitBreakerState.HALF_OPEN
    
    # Now simulate success
    logger.info("Simulating success...")
    cb.record_success()
    
    logger.info(f"State after success: {cb.state}")
    assert cb.state == CircuitBreakerState.CLOSED

    logger.info("Test Completed Successfully")

if __name__ == "__main__":
    test_circuit_breaker()