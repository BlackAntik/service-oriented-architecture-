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

