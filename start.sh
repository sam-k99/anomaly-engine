#!/bin/bash
echo "Starting Data Simulator..."
python src/producer.py &

echo "Starting ML Detection Engine..."
python src/ml_consumer.py &

echo "Starting AI Agent..."
python src/agent_consumer.py &

echo "Starting FastAPI Dashboard..."
exec uvicorn src.api.main:app --host 0.0.0.0 --port 8000
