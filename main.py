from flask import Flask, request, jsonify, render_template
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import json
from pydantic import ValidationError
from schemas.event import EventIngestSchema
from confluent_kafka import Producer

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)
app = Flask(__name__, template_folder='templates')

# API Security
API_KEY = os.getenv('API_KEY', 'default-dev-key')

@app.before_request
def require_api_key():
    if request.endpoint == 'ingest':
        provided_key = request.headers.get('x-api-key')
        if provided_key != API_KEY:
            return jsonify({"error": "Unauthorized. Invalid x-api-key header."}), 401

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')

kafka_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}

# Add SASL authentication if username is provided (for Cloud Kafka like Confluent)
if KAFKA_USERNAME and KAFKA_PASSWORD:
    kafka_config.update({
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN', # Confluent Cloud uses PLAIN
        'sasl.username': KAFKA_USERNAME,
        'sasl.password': KAFKA_PASSWORD
    })

producer = None


def delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA ERROR] message delivery failed: {err}")
    else:
        print(f"[KAFKA SUCCESS] delivered to {msg.topic()} [{msg.partition()}]")


def get_producer():
    global producer
    if producer is None:
        try:
            producer = Producer(kafka_config)
        except Exception as exc:
            print(f"[KAFKA INIT ERROR] {exc}")
            producer = False
    return producer if producer is not False else None


def persist_event(payload):
    event_record = {
        'id': str(payload.event_id),
        'timestamp': payload.timestamp,
        'service_name': payload.source,
        'environment': os.getenv('ENVIRONMENT', 'dev'),
        'event_type': payload.event_name,
        'message': json.dumps(payload.payload),
        'metadata': json.dumps(payload.payload),
    }
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO system_events (id, timestamp, service_name, environment, event_type, message, metadata)
                VALUES (:id, :timestamp, :service_name, :environment, :event_type, :message, :metadata)
            """),
            event_record,
        )


@app.route("/ingest", methods=["POST"])
def ingest_pipeline():
    try:
        raw_data = request.get_json(force=True)
        payload = EventIngestSchema(**raw_data)
    except ValidationError as exc:
        return jsonify({"error": exc.errors()}), 422
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    try:
        message_bytes = json.dumps(payload.model_dump(mode="json")).encode("utf-8")
        kafka_producer = get_producer()
        if kafka_producer is not None:
            try:
                kafka_producer.produce(
                    topic='system-events',
                    key=str(payload.event_id),
                    value=message_bytes,
                    callback=delivery_report,
                )
                kafka_producer.flush(timeout=2)
                return jsonify({"status": "Data received and queued in kafka ✌️!", "event_id": payload.event_id}), 200
            except Exception as exc:
                print(f"[KAFKA SEND ERROR] {exc}")

        persist_event(payload)
        return jsonify({"status": "Data received and stored locally ✌️!", "event_id": payload.event_id}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        







#  ------------------    DB health check for assurance
@app.route("/health")
def healthz():
    try:
        with engine.connect() as conn:
            value = conn.execute(text('SELECT 1')).scalar()
        return f'DB OK: {value}', 200
    except Exception as e:
        return f"DB error: {e}", 500


# test route
@app.route("/test", methods=["GET"])
def test():
    return "ok hello"

@app.route("/dashboard", methods=["GET"])
def dashboard():
    return render_template("dashboard.html")

@app.route("/api/events", methods=["GET"])
def get_events():
    try:
        with engine.connect() as conn:
            result = conn.execute(text('SELECT id, event_type, timestamp, service_name, message FROM system_events ORDER BY timestamp DESC LIMIT 50'))
            events = []
            for row in result:
                events.append({
                    "event_id": str(row[0]),
                    "event_name": row[1],
                    "timestamp": row[2].isoformat() if row[2] else None,
                    "source": row[3],
                    "payload": row[4]
                })
            return jsonify(events), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)