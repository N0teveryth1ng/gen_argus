from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from pydantic import ValidationError
from schemas.event import EventIngestSchema  # Still using Pydantic for ultra-fast validation
import asyncio
from confluent_kafka import producer



load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)
app = Flask(__name__, template_folder='templates')



# 1. Initialize the Kafka Producer once when the app starts
kafka_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(kafka_config)




# ingestion event 
@app.route("/ingest", methods=["POST"])
async def ingest_pipeline():


    raw_data = request.get_json(force=True)
    payload = EventIngestSchema(**raw_data)

     
    if not payload:
       return jsonify({"error:" "No data provided"}), 400  
         
         
    try:
        producer.produce(
            topic='system-events', 
            key=str(payload.id), 
            value=message_bytes,
            callback=delivery_report
        )
        
        # push the data to kafka
        producer.poll(0)
        
        return jsonify({"status": "Data received and queued in kafka ✌️!"}), 200


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