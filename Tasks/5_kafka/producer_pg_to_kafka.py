# producer_pg_to_kafka.py (простой, идемпотентность через sent_to_kafka)
import psycopg2
from kafka import KafkaProducer
import json
import time

# 1) Kafka: подключение и сериализация dict -> JSON -> bytes
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2) Postgres: подключаемся к базе и берём курсор
conn = psycopg2.connect(dbname="test_db", user="admin", password="admin",
                        host="localhost", port=5432)
conn.autocommit = False
cur = conn.cursor()

# 3) Забираем только НЕотправленные строки
cur.execute("""
    SELECT id, username, event_type, extract(epoch FROM event_time)::float AS ts
    FROM user_logins
    WHERE sent_to_kafka = false
    ORDER BY event_time
""")
rows = cur.fetchall()

# 4) Проходим по строкам: отправили -> отметили в PG -> коммит
try:
    for rid, user, event, ts in rows:
        message = {"user": user, "event": event, "timestamp": ts}
        producer.send("user_events", value=message)   # отправка в Kafka (асинхронно)
        print("Sent:", message)

        cur.execute("UPDATE user_logins SET sent_to_kafka = TRUE WHERE id = %s", (rid,))
        conn.commit()                                 # фиксируем отметку без батчей
        time.sleep(0.01)                              # чисто для наглядности
finally:
    producer.flush()  # дожимаем буфер перед выходом
    cur.close()
    conn.close()
