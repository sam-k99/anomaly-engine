import os
import json
import requests
from dotenv import load_dotenv
from confluent_kafka import Consumer
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

load_dotenv()

llm = ChatGroq(
    temperature=0,
    model_name="llama-3.1-8b-instant",
    groq_api_key=os.getenv("GROQ_API_KEY")
)
consumer_conf = {'bootstrap.servers': 'kafka:9092', 'group.id': 'agent-group', 'auto.offset.reset': 'earliest'}
consumer = Consumer(consumer_conf)
consumer.subscribe(['flagged-anomalies'])

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are an expert SOC (Security Operations Center) AI Agent. "
               "You receive anomalous user events from an Isolation Forest ML model. "
               "Analyze the event and determine if it is a real threat (like a bot or credential stuffing) or a false positive (just a slow human). "
               "Respond ONLY with a valid JSON object containing three keys: 'threat_level' (Low, Medium, High), 'assessment' (your reasoning), and 'recommendation' (what action to take)."),
    ("human", "Analyze this anomalous event:\n{event}")
])

parser = JsonOutputParser()
chain = prompt | llm | parser

def main():
    print("🤖 AI Agent Online. Listening for anomalies to investigate...")
    # FastAPI Webhook endpoint to push reports to the dashboard
    WEBHOOK_URL = "http://localhost:8000/webhook" # Keep as localhost because the Agent runs inside the same container as FastAPI!
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
                
            event = json.loads(msg.value().decode('utf-8'))
            print(f"[AGENT] Investigating anomaly for User {event['user_id']}...")
            
            try:
                response = chain.invoke({"event": json.dumps(event)})
                
                # Combine the event data with the AI report
                dashboard_payload = {
                    "user_id": event['user_id'],
                    "threat_level": response.get('threat_level', 'Unknown'),
                    "assessment": response.get('assessment', 'No assessment'),
                    "recommendation": response.get('recommendation', 'No recommendation')
                }
                
                # Push to FastAPI Dashboard
                requests.post(WEBHOOK_URL, json=dashboard_payload)
                print(f"[AGENT] Report pushed to dashboard for User {event['user_id']}.")
                
            except Exception as llm_error:
                print(f"[Agent Error] LLM failed: {llm_error}")
                
    except KeyboardInterrupt:
        print("\nStopping AI Agent...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
