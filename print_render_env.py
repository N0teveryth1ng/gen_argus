"""
Print Render environment variables ready to copy-paste.

Usage:
    .\.venv\Scripts\python.exe print_render_env.py

Reads values from .env in the project root.
"""

from dotenv import load_dotenv
import os

load_dotenv()

REQUIRED_KEYS = [
    "DATABASE_URL",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_USERNAME",
    "KAFKA_PASSWORD",
    "API_KEY",
    "ENVIRONMENT",
]

missing = [key for key in REQUIRED_KEYS if not os.getenv(key)]
if missing:
    print("Missing in .env:", ", ".join(missing))
    raise SystemExit(1)

print("=" * 60)
print("RENDER ENV VARS - copy everything below this line")
print("Paste into BOTH services: telemetry-api + telemetry-consumer")
print("=" * 60)
print()

for key in REQUIRED_KEYS:
    print(f"{key}={os.getenv(key)}")

print()
print("=" * 60)
print("Done. Add each line in Render -> Environment -> Add Environment Variable")
print("=" * 60)
