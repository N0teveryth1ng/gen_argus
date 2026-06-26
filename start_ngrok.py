from pyngrok import ngrok, conf
import time

# Set the Ngrok Auth Token
conf.get_default().auth_token = "3FdZhLWh9MbY3xaxv1ZDpvCwL2m_65ru4G2Hiso69iDRptr4s"

# We are going to tunnel the Flask API running on port 5000
port = 5000

print("Starting Ngrok tunnel...")
# Open a HTTP tunnel on the default port 80
# <NgrokTunnel: "http://<public_sub>.ngrok.io" -> "http://localhost:5000">
public_url = ngrok.connect(port)

print("\n" + "="*50)
print(f"??? PUBLIC URL READY: {public_url.public_url}")
print("="*50)
print("\nGive this URL to your interviewer! They can send POST requests to:")
print(f"{public_url.public_url}/ingest\n")

print("Keeping the tunnel open. Press CTRL+C to close.")

try:
    # Keep the script running to maintain the tunnel
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Closing tunnel...")
    ngrok.kill()
