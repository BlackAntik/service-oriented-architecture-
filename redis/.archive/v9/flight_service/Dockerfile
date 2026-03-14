FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*

COPY flight_service/requirements.txt /app/flight_service/requirements.txt
RUN pip install --no-cache-dir -r /app/flight_service/requirements.txt

COPY proto /app/proto
COPY flight_service /app/flight_service

RUN mkdir -p /app/flight_service/app/gen

RUN python -m grpc_tools.protoc \
      -I/app/proto \
      --python_out=/app/flight_service/app/gen \
      --grpc_python_out=/app/flight_service/app/gen \
      /app/proto/flight/v1/flight_service.proto

RUN python -c "import pathlib; \
p=pathlib.Path('/app/flight_service/app/gen'); \
[p2.mkdir(parents=True, exist_ok=True) for p2 in [p, p/'flight', p/'flight'/'v1']]; \
[(p2/'__init__.py').touch(exist_ok=True) for p2 in [p, p/'flight', p/'flight'/'v1']]"

ENV PYTHONPATH="/app/flight_service/app:/app/flight_service/app/gen"

EXPOSE 50051

CMD ["sh", "-c", "alembic -c /app/flight_service/alembic.ini upgrade head && python /app/flight_service/app/grpc_server.py"]