import urllib.request
import json
from datetime import datetime
import uuid

url = "https://rebuff-ogle-chevy.ngrok-free.dev/ingest"

payload = {
    "event_id": str(uuid.uuid4()),
    "event_name": "user_signup",
    "timestamp": datetime.utcnow().isoformat() + "Z",
    "source": "web_frontend",
    "payload": {
        "user_id": 101,
        "email": "test@example.com"
    }
}

print(f"Sending POST request to {url}...")
print(f"Payload: {json.dumps(payload, indent=2)}")

data = json.dumps(payload).encode('utf-8')

# The default key we added to main.py
API_KEY = "default-dev-key"

headers = {
    'Content-Type': 'application/json',
    'x-api-key': API_KEY
}

req = urllib.request.Request(url, data=data, headers=headers)

try:
    with urllib.request.urlopen(req) as response:
        print(f"\nStatus Code: {response.status}")
        print(f"Response: {response.read().decode('utf-8')}")
except urllib.error.HTTPError as e:
    print(f"\nHTTP Error: {e.code}")
    print(f"Response: {e.read().decode('utf-8')}")
except Exception as e:
    print(f"\nError: {e}")

