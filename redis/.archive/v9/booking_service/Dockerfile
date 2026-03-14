FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*

COPY booking_service/requirements.txt /app/booking_service/requirements.txt
RUN pip install --no-cache-dir -r /app/booking_service/requirements.txt

COPY proto /app/proto
COPY booking_service /app/booking_service

RUN mkdir -p /app/booking_service/app/gen

RUN python -m grpc_tools.protoc \
      -I/app/proto \
      --python_out=/app/booking_service/app/gen \
      --grpc_python_out=/app/booking_service/app/gen \
      /app/proto/flight/v1/flight_service.proto

RUN python -c "import pathlib; \
p=pathlib.Path('/app/booking_service/app/gen'); \
[p2.mkdir(parents=True, exist_ok=True) for p2 in [p, p/'flight', p/'flight'/'v1']]; \
[(p2/'__init__.py').touch(exist_ok=True) for p2 in [p, p/'flight', p/'flight'/'v1']]"

ENV PYTHONPATH="/app/booking_service/app:/app/booking_service/app/gen"

EXPOSE 8000

CMD ["sh", "-c", "alembic -c /app/booking_service/alembic.ini upgrade head && uvicorn main:app --host ${HTTP_HOST:-0.0.0.0} --port ${HTTP_PORT:-8000}"]
