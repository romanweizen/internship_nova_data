# consumer_to_clickhouse.py
from kafka import KafkaConsumer
import json
import clickhouse_connect
from datetime import datetime, timezone

# --- Kafka consumer ---
consumer = KafkaConsumer(
    "user_events",                     # имя топика
    bootstrap_servers="localhost:9092",# брокер
    group_id="ch-writer",              # ID группы: сохраняет оффсет (прогресс чтения)
    auto_offset_reset="earliest",      # если оффсета ещё нет для ЭТОЙ группы -> читать с начала
    enable_auto_commit=True,           # автофиксация оффсетов в Kafka (__consumer_offsets)
    value_deserializer=lambda b: json.loads(b.decode("utf-8"))  # bytes -> str -> dict(JSON)
)

# --- ClickHouse client (HTTP, порт 8123) ---
client = clickhouse_connect.get_client(
    host='localhost', port=8123, username='user', password='strongpassword'
)

# --- Создаём таблицу при старте (идемпотентно) ---
client.command("""
CREATE TABLE IF NOT EXISTS user_logins (
    username String,       -- поле из JSON 'user'
    event_type String,     -- поле из JSON 'event'
    event_time DateTime    -- время события (CH тип)
) ENGINE = MergeTree
ORDER BY event_time
""")

print("Waiting for messages… (Ctrl+C to stop)")

try:
    for msg in consumer:
        data = msg.value              # уже dict: {'user':..., 'event':..., 'timestamp':...}
        print("Received:", data)

        # Преобразуем Unix time -> DateTime (UTC), затем делаем naive datetime (CH так удобнее)
        dt = datetime.fromtimestamp(float(data["timestamp"]), tz=timezone.utc).replace(tzinfo=None)

        # Вставка одной строки (для учебного примера без батчей):
        client.insert(
            "user_logins",
            [(data["user"], data["event"], dt)],
            column_names=["username", "event_type", "event_time"]
        )

except KeyboardInterrupt:
    print("\nStopping consumer…")

finally:
    consumer.close()  # закрываем консьюмера аккуратно
