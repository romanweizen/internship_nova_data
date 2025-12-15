# PostgreSQL → Kafka → ClickHouse (без дубликатов)

## Описание

Проект демонстрирует простой streaming-pipeline для переноса данных
из PostgreSQL в ClickHouse через Kafka.

Особенности решения:
- Kafka используется как брокер сообщений
- Producer не отправляет повторно уже обработанные записи
- Consumer читает данные из Kafka и сохраняет их в ClickHouse
- Вся инфраструктура поднимается через Docker Compose

---

## Архитектура

```

PostgreSQL → Kafka (user_events) → ClickHouse

```

---

## Структура проекта

```

.
├── docker-compose.yml
├── init.sql
├── producer_pg_to_kafka.py
├── consumer_to_clickhouse.py
└── README.md

````

---

## Краткое описание файлов

- **docker-compose.yml**  
  Поднимает PostgreSQL, Kafka, Zookeeper и ClickHouse.

- **init.sql**  
  Создаёт таблицу `user_logins` в PostgreSQL и добавляет тестовые данные.
  Выполняется автоматически при первом запуске контейнера Postgres.

- **producer_pg_to_kafka.py**  
  Читает данные из PostgreSQL и отправляет их в Kafka.  
  Отправляются только записи с `sent_to_kafka = false`.  
  После отправки запись помечается как отправленная.

- **consumer_to_clickhouse.py**  
  Читает сообщения из Kafka и сохраняет их в ClickHouse.  
  Таблица в ClickHouse создаётся автоматически при запуске.

---

## Запуск проекта

### 1. Поднять инфраструктуру
```bash
docker compose up -d
````

---


### 2. Запустить consumer

```bash
python consumer_to_clickhouse.py
```

---

### 3. Запустить producer

```bash
python producer_pg_to_kafka.py
```

---

## Проверка данных через DBeaver

### PostgreSQL

* Host: `localhost`
* Port: `5432`
* Database: `test_db`
* User: `admin`
* Password: `admin`

Проверка:

```sql
SELECT * FROM user_logins;
```

Поле `sent_to_kafka` показывает, какие записи уже были отправлены в Kafka.

---

### ClickHouse

* Host: `localhost`
* Port: `8123`
* User: `user`
* Password: `strongpassword`

Проверка:

```sql
SELECT * FROM user_logins;
```

---

## Повторный запуск

* Повторный запуск producer не приводит к дубликатам —
  все отправленные записи помечаются флагом `sent_to_kafka`.
* Consumer можно останавливать и запускать повторно,
  Kafka сохраняет прогресс чтения.

---

## Итог

Реализован устойчивый и воспроизводимый pipeline
для миграции данных из PostgreSQL в ClickHouse с защитой от дубликатов.