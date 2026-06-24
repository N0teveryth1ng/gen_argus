from confluent_kafka import Consumer

# 1. Configure the consumer with a unique group ID
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-python-group',
    'auto.offset.reset': 'earliest' # Start reading from the very beginning
}
consumer = Consumer(config)

# 2. Subscribe to the mailbox topic
consumer.subscribe(['user-signups'])

print("Waiting for messages... Press Ctrl+C to exit.")


try:
    while True:
        # poll the server for messages
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"consumer error.{msg.error()}")
            continue    
        
        print(f"Received message: {msg.value().decode('utf-8')}") 
        
        
        
except Exception as e:
    print(f" system error: {e}")
finally:
    consumer.close()
    
    
    