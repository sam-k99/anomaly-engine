import os
import json
import time
import random
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# Kafka configuration
conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)

# Topic name
TOPIC = 'raw-user-events'

def generate_event():
    """Generates a single realistic or anomalous user event."""
    user_id = random.randint(1000, 9999)
    actions = ['login', 'click_button', 'view_page', 'add_to_cart', 'logout']
    
    # 5% chance to generate a "bot" event (super fast, repetitive actions)
    is_bot = random.random() < 0.05
    
    if is_bot:
        action = 'api_request'
        # Bots do things in milliseconds
        processing_time_ms = random.randint(1, 5) 
    else:
        action = random.choice(actions)
        # Humans take longer
        processing_time_ms = random.randint(500, 3000)

    event = {
        "user_id": user_id,
        "action": action,
        "ip_address": fake.ipv4(),
        "timestamp": datetime.now().isoformat(),
        "processing_time_ms": processing_time_ms,
        "is_bot_injected": is_bot  # We will use this later to prove our ML model catches it
    }
    return event

def delivery_report(err, msg):
    """Callback for Kafka delivery reports."""
    if err is not None:
        print(f"[Kafka Error] Delivery failed: {err}")
    else:
        print(f"[Kafka] Sent to {msg.topic()} [Partition {msg.partition()}]")

def main():
    print(f"Starting Data Simulator. Publishing to topic: {TOPIC}")
    try:
        while True:
            event = generate_event()
            
            # If it's a bot, let's simulate a burst of 20 requests in 1 second
            burst_count = 20 if event["is_bot_injected"] else 1
            
            for _ in range(burst_count):
                event["timestamp"] = datetime.now().isoformat()
                producer.produce(
                    TOPIC, 
                    key=str(event["user_id"]), 
                    value=json.dumps(event), 
                    callback=delivery_report
                )
                producer.poll(0) # Trigger delivery callbacks
            
            time.sleep(random.uniform(0.5, 2.0)) # Human pause
            
    except KeyboardInterrupt:
        print("\nStopping simulator...")
        producer.flush()

if __name__ == "__main__":
    main()
