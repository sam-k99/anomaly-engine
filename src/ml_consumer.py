import json
import time
from datetime import datetime
from confluent_kafka import Consumer, Producer
from sklearn.ensemble import IsolationForest
import numpy as np

# Kafka Consumer Config
consumer_conf = {'bootstrap.servers': 'kafka:9092', 'group.id': 'ml-group', 'auto.offset.reset': 'earliest'}
consumer = Consumer(consumer_conf)
consumer.subscribe(['raw-user-events'])

# Kafka Producer Config (To push anomalies to a new topic)
producer = Producer({'bootstrap.servers': 'kafka:9092'})

# ML Model & State
model = IsolationForest(contamination=0.02, random_state=42)
training_buffer = []
is_model_trained = False
user_last_event_time = {}

# Map actions to numbers for the ML model
action_mapping = {
    'login': 1, 'click_button': 2, 'view_page': 3, 
    'add_to_cart': 4, 'logout': 5, 'api_request': 6
}

def extract_features(event):
    """Converts JSON event into a numerical array for scikit-learn."""
    user_id = event['user_id']
    current_time = datetime.fromisoformat(event['timestamp'])
    
    # Calculate time since user's last event (Velocity)
    last_time = user_last_event_time.get(user_id)
    if last_time:
        time_delta = (current_time - last_time).total_seconds() * 1000 # in ms
    else:
        time_delta = 5000 # Default 5 seconds for new users
        
    user_last_event_time[user_id] = current_time
    
    action_code = action_mapping.get(event['action'], 0)
    
    # Features: [processing_time_ms, time_delta_ms, action_code]
    return np.array([[event['processing_time_ms'], time_delta, action_code]])

def main():
    global is_model_trained, training_buffer
    
    print("Starting ML Consumer. Listening for events...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
                
            event = json.loads(msg.value().decode('utf-8'))
            features = extract_features(event)
            
            if not is_model_trained:
                # WARMUP PHASE: Collect 50 events to learn normal behavior
                training_buffer.append(features[0])
                if len(training_buffer) >= 200:
                    print("[ML Model] Training Isolation Forest on 50 baseline events...")
                    model.fit(np.array(training_buffer))
                    is_model_trained = True
                    print("[ML Model] Model trained! Now scoring live events for anomalies...")
                continue
                
            # SCORING PHASE: Predict anomaly
            prediction = model.predict(features)
            
            if prediction[0] == -1:
                print(f"🚨 [ANOMALY DETECTED] User {event['user_id']} did {event['action']} in {event['processing_time_ms']}ms")
                
                # Push the flagged anomaly to a new Kafka topic for the AI Agent
                producer.produce('flagged-anomalies', value=json.dumps(event))
                producer.poll(0)
            else:
                # Normal event, just log it quietly
                print(f"✅ [Normal] User {event['user_id']} did {event['action']}")
                
    except KeyboardInterrupt:
        print("\nStopping ML Consumer...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    main()
