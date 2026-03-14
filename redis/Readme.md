## Пункт 2

Диаграмму можно посмотреть на сайте https://dbdiagram.io

## Пункт 3

#### Запуск

```bash
docker-compose -f homework3/docker-compose.yml up -d --build
```

#### Остановка

```bash
docker-compose -f homework3/docker-compose.yml down
```

#### Сваггер

http://localhost:8000/docs

#### Посмотреть что внутри базы данных

```bash
docker-compose -f homework3/docker-compose.yml exec booking-db psql -U booking -d bookingdb
```

# Потестить задание 5

```bash
docker-compose -f homework3/docker-compose.yml exec flight-db psql -U flight -d flightdb
```

```sql
SELECT * FROM flights;
```

```sql
UPDATE flights SET available_seats = 1 WHERE id = '10000000-0000-0000-0000-000000000002';
```

3) Запустить тестовый скрипт

```bash
export FLIGHT_ID='10000000-0000-0000-0000-000000000002' && python3 homework3/tests/test_race_condition.py
```

## Потестить задание 6

Успешный запрос

```bash
grpcurl -plaintext \
  -proto homework3/proto/flight/v1/flight_service.proto \
  -H 'x-api-key: secret-api-key-123' \
  -d '{"origin": "SVO", "destination": "LED"}' \
  localhost:50051 \
  flight.v1.FlightService/SearchFlights
```

Неуспешный запрос

```bash
grpcurl -plaintext -proto homework3/proto/flight/v1/flight_service.proto -H 'x-api-key: wrong-key' -d '{"origin": "SVO", "destination": "LED"}' localhost:50051 flight.v1.FlightService/SearchFlights
```

## Потестить задание 7

```bash
curl -X 'GET' \
  'http://localhost:8000/flights?origin=SVO&destination=LED' \
  -H 'accept: application/json'
```

```bash
curl -X 'POST' \
  'http://localhost:8000/bookings' \
  -H 'Content-Type: application/json' \
  -d '{
  "user_id": "test_user",
  "flight_id": "10000000-0000-0000-0000-000000000001",
  "passenger_name": "Cache Tester",
  "passenger_email": "test@example.com",
  "seat_count": 1
}'
```

## Потестить задание 8

```bash
docker-compose -f homework3/docker-compose.yml stop flight-service
```

```bash
time curl -X 'GET' \
  'http://localhost:8000/flights?origin=MOW&destination=LED' \
  -H 'accept: application/json'
```

```bash
docker-compose -f homework3/docker-compose.yml start flight-service
```

```bash
export FLIGHT_ID="10000000-0000-0000-0000-000000000001"
export BOOKING_ID=$(uuidgen)
```

```bash
grpcurl -plaintext \
  -proto homework3/proto/flight/v1/flight_service.proto \
  -H 'x-api-key: secret-api-key-123' \
  -d "{\"flight_id\": \"$FLIGHT_ID\", \"booking_id\": \"$BOOKING_ID\", \"seats\": 1}" \
  localhost:50051 \
  flight.v1.FlightService/ReserveSeats
```

## Задание 9

Проверим, какие контейнеры запущены

```bash
docker-compose -f homework3/docker-compose.yml ps | grep redis
```

```bash
curl -X 'GET' 'http://localhost:8000/flights?origin=MOW&destination=LED'
```

```bash
docker-compose -f homework3/docker-compose.yml stop redis-master
```

```bash
docker-compose -f homework3/docker-compose.yml logs redis-sentinel
```

```bash
docker-compose -f homework3/docker-compose.yml start redis-master
```

## Задание 10

```bash
python3 homework3/tests/test_circuit_breaker.py
```