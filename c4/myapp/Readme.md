# Health сервис

Простой HTTP-сервис, который возвращает `200 OK` на эндпоинт `/health`.

## Сборка докер образа

```bash
docker build -t health-service .
```

## Запустить сервис

```bash
docker run -d -p 8080:5000 --name my-health-service health-service
```

## Остановить сервис

```bash
docker stop my-health-service
```
