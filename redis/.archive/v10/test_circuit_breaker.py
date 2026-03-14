import time
import grpc
import uuid
import logging
import os
import sys

# Add booking_service/app to path to import modules
sys.path.append(os.path.join(os.getcwd(), "homework3/booking_service/app"))

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
    # Note: This will try to connect to localhost:50051. 
    # If flight-service is running in docker, we need to map the port or run this inside docker.
    # Assuming we run this locally and port 50051 is mapped.
    client = FlightClient()
    
    # Access internal circuit breaker for testing state
    cb = client._circuit_breaker
    
    logger.info(f"Initial State: {cb.state}")
    assert cb.state == CircuitBreakerState.CLOSED
    
    # 1. Simulate failures to trip the breaker
    logger.info("Simulating failures...")
    
    # We need to make calls that fail. 
    # If flight service is UP, we can't easily force it to fail unless we stop it.
    # Or we can mock the stub.
    
    # Let's mock the stub to raise UNAVAILABLE
    original_stub = client._stub
    
    class MockStub:
        def SearchFlights(self, request, metadata):
            raise grpc.RpcError(grpc.StatusCode.UNAVAILABLE, "Service Unavailable")
            
    client._stub = MockStub()
    
    # Trip the breaker (Threshold is 3)
    for i in range(3):
        try:
            logger.info(f"Request {i+1}")
            client.search_flights("MOW", "LED", None)
        except Exception as e:
            logger.info(f"Caught expected error: {e}")
            
    logger.info(f"State after 3 failures: {cb.state}")
    assert cb.state == CircuitBreakerState.OPEN
    
    # 2. Verify OPEN state blocks requests immediately
    logger.info("Verifying OPEN state blocking...")
    start_time = time.time()
    try:
        client.search_flights("MOW", "LED", None)
    except Exception as e:
        logger.info(f"Caught error in OPEN state: {e}")
        # Should be fast, no sleep/retry delay from retry decorator if it was blocked by CB first?
        # Wait, retry decorator is outside? 
        # @retry_grpc wraps the method. Inside method calls self._stub.SearchFlights.
        # Interceptor is on the channel.
        # So: client.search_flights -> retry_wrapper -> method -> stub -> interceptor -> channel
        # If interceptor raises UNAVAILABLE, retry wrapper will catch it and retry!
        # This is interesting. If CB is OPEN, it raises UNAVAILABLE. Retry wrapper sees UNAVAILABLE and retries.
        # So it will retry 3 times against the OPEN breaker.
        # This is acceptable, but maybe inefficient. 
        # Ideally CB should be outside retry? Or retry should know about CB?
        # Usually Retry is around the network call. CB is also around network call.
        # If CB is OPEN, we want to fail fast.
        # If Retry wraps CB, it will retry the "Fast Fail".
        pass
        
    duration = time.time() - start_time
    logger.info(f"Request in OPEN state took {duration:.4f}s")
    
    # 3. Wait for recovery timeout (5s)
    logger.info("Waiting for recovery timeout (5s)...")
    time.sleep(6)
    
    # 4. Verify HALF_OPEN
    # The next request should be allowed.
    # We want this one to SUCCEED to close the breaker.
    # Restore original stub (assuming real service is up)
    # If real service is NOT up, it will fail and go back to OPEN.
    
    # Let's keep it failing to see it go back to OPEN first.
    logger.info("Testing HALF_OPEN -> OPEN transition")
    try:
        client.search_flights("MOW", "LED", None)
    except Exception:
        pass
        
    logger.info(f"State after failure in HALF_OPEN: {cb.state}")
    assert cb.state == CircuitBreakerState.OPEN
    
    # Wait again
    logger.info("Waiting for recovery timeout again (5s)...")
    time.sleep(6)
    
    # Now restore stub to succeed
    # We need a real connection or a mock success
    class MockSuccessStub:
        def SearchFlights(self, request, metadata):
            # Return empty response
            from flight.v1 import flight_service_pb2
            return flight_service_pb2.SearchFlightsResponse(flights=[])

    client._stub = MockSuccessStub()
    
    logger.info("Testing HALF_OPEN -> CLOSED transition")
    try:
        client.search_flights("MOW", "LED", None)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        
    logger.info(f"State after success: {cb.state}")
    assert cb.state == CircuitBreakerState.CLOSED

    logger.info("Test Completed Successfully")

if __name__ == "__main__":
    test_circuit_breaker()