import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))
with engine.connect() as conn:
    print(conn.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='system_events' ORDER BY ordinal_position")).fetchall())
    try:
        conn.execute(
            text("INSERT INTO system_events (id, timestamp, service_name, environment, event_type, message, metadata) VALUES (:id, :timestamp, :service_name, :environment, :event_type, :message, :metadata)"),
            {
                'id': 'test-001',
                'timestamp': '2026-06-24T12:00:00Z',
                'service_name': 'demo',
                'environment': 'dev',
                'event_type': 'signup',
                'message': '{}',
                'metadata': {},
            }
        )
        conn.commit()
        print('INSERT_OK')
    except Exception as e:
        print(type(e).__name__, e)
