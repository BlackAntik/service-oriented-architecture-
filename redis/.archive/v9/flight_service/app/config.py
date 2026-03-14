import os


def getenv_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


DATABASE_URL = getenv_required("DATABASE_URL")
REDIS_SENTINEL_HOST = os.getenv("REDIS_SENTINEL_HOST", "localhost")
REDIS_SENTINEL_PORT = int(os.getenv("REDIS_SENTINEL_PORT", "26379"))
REDIS_MASTER_SET = os.getenv("REDIS_MASTER_SET", "mymaster")

GRPC_HOST = os.getenv("GRPC_HOST", "0.0.0.0")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))

RESERVATION_TTL_SECONDS = int(os.getenv("RESERVATION_TTL_SECONDS", "900"))

FLIGHT_SERVICE_API_KEY = getenv_required("FLIGHT_SERVICE_API_KEY")
