from confluent_kafka import Consumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import json
import time
import uuid

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)

config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': 'my-python-group',
    'auto.offset.reset': 'earliest',
}
consumer = Consumer(config)
consumer.subscribe(['system-events'])

print("Waiting for messages... Press Ctrl+C to exit.")

batch_buffer = []
MAX_BATCH_SIZE = 1000
MAX_WAIT_TIME = 2.0
last_flush_time = time.time()


def flush_to_destination():
    global batch_buffer, last_flush_time
    if not batch_buffer:
        last_flush_time = time.time()
        return

    try:
        print(f"Flushing {len(batch_buffer)} records to database")
        with engine.begin() as conn:
            query = text("""
                INSERT INTO system_events (id, timestamp, service_name, environment, event_type, message, metadata)
                VALUES (:id, :timestamp, :service_name, :environment, :event_type, :message, :metadata)
            """)
            conn.execute(query, batch_buffer)

        batch_buffer.clear()
        last_flush_time = time.time()

    except Exception as e:
        print(f"Storage Error: {e}")


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            if time.time() - last_flush_time >= MAX_WAIT_TIME:
                flush_to_destination()
            continue

        if msg.error():
            print(f"consumer error: {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
        except Exception as e:
            print(f"message parse error: {e}")
            continue

        batch_buffer.append({
            'id': str(uuid.uuid4()),
            'timestamp': data.get('timestamp'),
            'service_name': data.get('source') or 'unknown',
            'environment': 'dev',
            'event_type': data.get('event_name') or 'INFO',
            'message': json.dumps(data.get('payload', {})),
            'metadata': json.dumps(data.get('payload', {})),
        })

        if len(batch_buffer) >= MAX_BATCH_SIZE:
            flush_to_destination()
        elif time.time() - last_flush_time >= MAX_WAIT_TIME:
            flush_to_destination()

except KeyboardInterrupt:
    print("Stopping consumer")
finally:
    consumer.close()
