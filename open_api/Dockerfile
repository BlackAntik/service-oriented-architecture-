FROM python:3.11-slim

WORKDIR /app

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем OpenAPI спецификацию
COPY src/main/resources/openapi/openapi.yaml src/main/resources/openapi/openapi.yaml

# Копируем скрипт генерации
COPY generate.sh .
RUN chmod +x generate.sh

# Генерируем код из OpenAPI спецификации
RUN ./generate.sh

# Копируем исходный код приложения
COPY app/ app/

# Открываем порт
EXPOSE 8000

# Запускаем приложение
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]