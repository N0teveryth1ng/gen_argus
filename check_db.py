import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))
with engine.connect() as conn:
    res = conn.execute(text("SELECT id, service_name, event_type FROM system_events ORDER BY timestamp DESC LIMIT 5")).fetchall()
    for r in res:
        print(r)
