import os


def getenv_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


DATABASE_URL = getenv_required("DATABASE_URL")
FLIGHT_GRPC_ADDR = getenv_required("FLIGHT_GRPC_ADDR")
