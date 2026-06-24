from flask import Flask, request, jsonify
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

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
kafka_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(kafka_config)


def delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA ERROR] message delivery failed: {err}")
    else:
        print(f"[KAFKA SUCCESS] delivered to {msg.topic()} [{msg.partition()}]")




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
        producer.produce(
            topic='system-events',
            key=str(payload.event_id),
            value=message_bytes,
            callback=delivery_report,
        )
        producer.poll(0)
        return jsonify({"status": "Data received and queued in kafka ✌️!", "event_id": payload.event_id}), 200
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




if __name__ == '__main__':
    app.run(debug=True)