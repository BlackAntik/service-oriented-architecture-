#!/bin/bash

set -e

mkdir -p generated/models

echo "Генерация моделей..."
datamodel-codegen \
  --input src/main/resources/openapi/openapi.yaml \
  --output generated/models/schemas.py \
  --output-model-type pydantic_v2.BaseModel \
  --field-constraints \
  --use-standard-collections \
  --use-schema-description \
  --use-title-as-name \
  --target-python-version 3.11

touch generated/__init__.py
touch generated/models/__init__.py

echo "Генерация кода завершена!"