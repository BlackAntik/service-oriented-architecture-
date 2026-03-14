import os


def getenv_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


DATABASE_URL = getenv_required("DATABASE_URL")
GRPC_HOST = os.getenv("GRPC_HOST", "0.0.0.0")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))

RESERVATION_TTL_SECONDS = int(os.getenv("RESERVATION_TTL_SECONDS", "900"))
