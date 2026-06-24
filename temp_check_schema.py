import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))
with engine.connect() as conn:
    print(conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('system_events','events') ORDER BY table_name")).fetchall())
    print(conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='system_events' ORDER BY ordinal_position")).fetchall())
