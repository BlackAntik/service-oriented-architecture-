import os


def getenv_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


DATABASE_URL = getenv_required("DATABASE_URL")
FLIGHT_GRPC_ADDR = getenv_required("FLIGHT_GRPC_ADDR")
FLIGHT_SERVICE_API_KEY = getenv_required("FLIGHT_SERVICE_API_KEY")

CIRCUIT_BREAKER_FAILURE_THRESHOLD = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5"))
CIRCUIT_BREAKER_RECOVERY_TIMEOUT = int(os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "10"))
