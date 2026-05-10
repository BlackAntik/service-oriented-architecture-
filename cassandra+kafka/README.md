# Smart Warehouse: Event-Driven State Management with Cassandra

## Запуск

```bash
docker-compose up
```

## Модель данных Cassandra

### Принципы проектирования

Cassandra не поддерживает JOIN и вторичные индексы с высокой кардинальностью эффективно. Схема проектируется **под запросы**: одна таблица — один паттерн доступа. Денормализация данных (дублирование) — намеренная и необходимая.

---

### Таблица `inventory_by_product_zone`

```cql
PRIMARY KEY (sku, zone_id)
```

**Partition key:** `sku` — все данные по одному товару хранятся на одном узле.  
**Clustering key:** `zone_id` — строки внутри партиции отсортированы по зоне, что позволяет эффективно читать как конкретную зону, так и все зоны товара.

**Поддерживаемый запрос:**
```cql
SELECT * FROM inventory_by_product_zone WHERE sku = ?;
SELECT * FROM inventory_by_product_zone WHERE sku = ? AND zone_id = ?;
```

---

### Таблица `inventory_by_product`

```cql
PRIMARY KEY (sku)
```

**Агрегированный взгляд** на остатки товара по всем зонам: одна строка на `sku` с полями `total_available` и `total_reserved` — суммой по всем зонам.

**Partition key:** `sku` — для запроса по конкретному товару нужен ровно один lookup на одну партицию, без вычитываний нескольких строк.

При обработке события consumer:
1. Читает все строки `inventory_by_product_zone WHERE sku=?` (партиция небольшая — товар хранится в ограниченном числе зон).
2. Вычисляет суммы с учётом нового значения для текущей зоны.
3. Атомарно (LOGGED BATCH) обновляет `inventory_by_product_zone`, `inventory_by_zone` и `inventory_by_product` — ни одна из таблиц не остаётся в рассинхроне.

**Поддерживаемый запрос:**
```cql
SELECT total_available, total_reserved FROM inventory_by_product WHERE sku = ?;
```

---

### Таблица `inventory_by_zone`

```cql
PRIMARY KEY (zone_id, sku)
```

**Partition key:** `zone_id` — все товары одной зоны на одном узле.  
**Clustering key:** `sku` — строки отсортированы по артикулу, поддерживается range scan.

**Поддерживаемый запрос:**
```cql
SELECT * FROM inventory_by_zone WHERE zone_id = ?;
```

---

### Таблица `event_log`

```cql
PRIMARY KEY (sku, occurred_at, event_id)
CLUSTERING ORDER BY (occurred_at DESC, event_id ASC)
```

**Partition key:** `sku` — история событий по конкретному товару на одном узле.  
**Clustering key:** `occurred_at DESC` — последние события читаются первыми (наиболее частый запрос для аудита).  
`event_id` добавлен для уникальности строки при одинаковом timestamp.

**Поддерживаемый запрос:**
```cql
SELECT * FROM event_log WHERE sku = ?;
SELECT * FROM event_log WHERE sku = ? AND occurred_at > ?;
```

---

### Таблица `orders`

```cql
PRIMARY KEY (order_id)
```

Заказы читаются по `order_id` — простой lookup, partition key достаточен.

---

### Таблица `processed_events`

```cql
PRIMARY KEY (event_id)
```

Используется для идемпотентности: быстрая проверка по `event_id` перед обработкой.

---

## Кластер Cassandra

Кластер состоит из 3 нод: `cassandra-1`, `cassandra-2`, `cassandra-3` в одном датацентре `dc1`.

Keyspace создаётся с `NetworkTopologyStrategy` и `replication_factor = 3`:

```cql
CREATE KEYSPACE IF NOT EXISTS warehouse
    WITH replication = {
        'class': 'NetworkTopologyStrategy',
        'dc1': 3
    };
```

Это означает, что каждая строка данных хранится на всех 3 нодах. При остановке одной ноды (`docker stop cassandra-2`) система продолжает работу, так как данные доступны на оставшихся 2 нодах, а QUORUM (2 из 3) по-прежнему достижим.

---

## Consistency Levels

### Запись: QUORUM

Для всех операций записи используется `QUORUM` (2 из 3 нод должны подтвердить запись).

**Обоснование:** QUORUM гарантирует, что данные записаны на большинство нод до подтверждения. Это обеспечивает устойчивость к потере одной ноды без потери данных. Для складской системы корректность остатков важнее скорости записи.

### Чтение: QUORUM

Для операций чтения (в `get_inventory`) также используется `QUORUM`.

**Обоснование:** Комбинация QUORUM write + QUORUM read гарантирует **strong consistency** — чтение всегда возвращает последнее записанное значение. Это критично для корректной обработки событий вне порядка (проверка `last_event_timestamp`): если бы чтение было `ONE`, могло бы вернуться устаревшее значение с ноды, которая ещё не получила последнюю запись, что привело бы к некорректной проверке staleness.

**Trade-off:** QUORUM read медленнее ONE (требует ответа от 2 нод вместо 1), но для данной системы это приемлемо, так как события обрабатываются с задержкой 2 секунды между ними.

---

## Архитектура

| Компонент | Роль |
|---|---|
| WMS Producer | Генерирует складские события, публикует в `warehouse-events` |
| Kafka | Брокер сообщений, топик `warehouse-events` (3 партиции) |
| Schema Registry | Версионирование Avro-схем |
| Consumer | Читает события, обновляет состояние в Cassandra (at-least-once) |
| Cassandra (3 ноды) | Хранение текущего состояния склада с RF=3 |
| DLQ | Топик `warehouse-events-dlq` для проблемных событий |

## At-least-once семантика

Consumer использует `enable.auto.commit: False`. Offset коммитится синхронно (`asynchronous=False`) только после успешной записи в Cassandra. При рестарте обработка продолжается с последнего закоммиченного offset.

## Отказоустойчивость

При остановке одной ноды:
```bash
docker stop cassandra-2
```

Система продолжает работу:
- Cassandra driver автоматически перенаправляет запросы на оставшиеся ноды
- QUORUM (2 из 3) достижим при 2 живых нодах
- `DCAwareRoundRobinPolicy` обеспечивает балансировку нагрузки между нодами

---

## Schema Evolution

### Стратегия совместимости: BACKWARD

Используется стратегия **BACKWARD compatibility** в Schema Registry. Это означает:
- Новая версия схемы может читать данные, записанные старой версией
- Consumer, использующий новую схему (V2), корректно обрабатывает события, записанные по старой схеме (V1)
- Новые поля **обязаны** иметь значение по умолчанию (`"default": null`)

### Версии схем

**V1** — [`schemas/warehouse_event.avsc`](schemas/warehouse_event.avsc): базовая схема без `supplier_id`.

**V2** — [`schemas/warehouse_event_v2.avsc`](schemas/warehouse_event_v2.avsc): добавлено поле:
```json
{"name": "supplier_id", "type": ["null", "string"], "default": null}
```

### Обработка в consumer

- **V1-события** (без `supplier_id`): `event.get('supplier_id')` возвращает `None` → в Cassandra записывается `NULL`
- **V2-события** (с `supplier_id`): значение записывается в колонку `supplier_id` таблиц инвентаря

Логика в [`handle_product_received()`](consumer/consumer.py):
```python
supplier_id = event.get('supplier_id')  # None для V1, строка для V2
write_inventory(..., supplier_id=supplier_id)
```

### Пошаговая инструкция добавления новой версии события

1. Создать новый файл схемы (например, `schemas/warehouse_event_v3.avsc`) с новым полем и `"default": null`
2. Убедиться, что новое поле backward-compatible (имеет default)
3. Зарегистрировать схему в Schema Registry:
   ```bash
   curl -X POST http://localhost:8081/subjects/warehouse-events-value/versions \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d "{\"schema\": $(cat schemas/warehouse_event_v3.avsc | jq -Rs .)}"
   ```
4. Добавить колонку в Cassandra (online schema change, не требует рестарта):
   ```cql
   ALTER TABLE warehouse.inventory_by_product_zone ADD new_field TEXT;
   ```
5. Обновить consumer — добавить `event.get('new_field')` в соответствующий обработчик
6. Обновить producer — начать отправлять события с новым полем

### Автоматическая регистрация при старте

Сервис `schema-init` в docker-compose автоматически регистрирует V1 и V2 схемы при старте через [`producer/register_schemas.py`](producer/register_schemas.py).
