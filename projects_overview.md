# Project: Autonomous Real-Time Anomaly & Fraud Investigation Engine

## 1. Summary
The **Autonomous Real-Time Anomaly Engine** is an event-driven streaming pipeline that monitors live user behavior to detect fraud, bots, and system glitches in milliseconds. 

Unlike traditional batch-processing models that analyze "yesterday's data," this system ingests live clickstream/transaction data via Apache Kafka, scores it using an unsupervised machine learning model (Isolation Forest) in real-time, and triggers an **Autonomous AI Agent** to investigate flagged anomalies, generate forensic reports, and recommend mitigation actions.

## 2. The Problem Statement (The "Why")
Mid-sized enterprises (fintech, e-commerce, SaaS) face a critical vulnerability: they rely on **static rules** for security (e.g., "Block user if they make 5 transactions in 1 minute"). 
*   **Flaw 1:** Hackers easily bypass static rules by setting their bots to 4 transactions per minute.
*   **Flaw 2:** When an alert fires, a human Security Operations Center (SOC) analyst has to manually dig through databases to figure out if it's a real attack or just a user mashing the refresh button. This takes hours.

## 3. The Solution (The "What")
We are building a three-layer system:
1.  **The Radar (Streaming + ML):** A Kafka pipeline that ingests live events. An unsupervised ML model profiles user behavior and assigns an "anomaly score" in real-time. It doesn't use static rules; it learns what "normal" looks like and flags deviations.
2.  **The Autopilot (AI Agent):** When an anomaly is detected, an LLM-based AI Agent is automatically triggered. The Agent uses tools to query the database, pull the user's historical data, analyze the payload, and write a forensic incident report.
3.  **The Dashboard (UI):** A live interface where engineering teams can watch the events flowing, see the ML model scoring, and read the AI Agent's real-time investigation reports.


## 4. Tech Stack & Tools
*   **Streaming Broker:** `Apache Kafka` (Managed via Docker/KRaft mode).
*   **Stream Processing:** `confluent-kafka` python client.
*   **Machine Learning:** `scikit-learn` (Isolation Forest) or `PyOD` (Python Outlier Detection library).
*   **AI Agent Framework:** `LangGraph` or `LlamaIndex` (for the autonomous investigator agent).
*   **LLM Provider:** `Mistral AI` (using the `openai` compatible client, same as Project 1).
*   **Database:** `PostgreSQL` or `DuckDB` (for the Agent to query historical user data).
*   **API/UI:** `FastAPI` + `WebSockets` (to push live alerts to the browser).
*   **Containerization:** `Docker` & `Docker Compose`.

## 5. System Architecture

```text
[Python Data Simulator]
       | (Generates 1,000 events/sec: logins, clicks, purchases)
       v
+---------------------------------------------------+
|              Apache Kafka (Topics)                |
|   Topic 1: raw-user-events                        |
|   Topic 2: flagged-anomalies                      |
+---------------------------------------------------+
       | (Consumes raw events)
       v
+---------------------------------------------------+
|         Python ML Consumer (The Radar)            |
| 1. Ingest event                                   |
| 2. Extract features (velocity, frequency)         |
| 3. Score via Isolation Forest (Unsupervised ML)   |
| 4. IF NORMAL: Drop/Log                            |
| 5. IF ANOMALY: Push to Topic 2 (flagged-anomalies)|
+---------------------------------------------------+
       | (Consumes flagged anomalies)
       v
+---------------------------------------------------+
|         AI Agent Worker (The Autopilot)           |
| 1. Receives anomalous event                       |
| 2. Agent decides to use a "Tool"                  |
| 3. Tool: Queries PostgreSQL for user history      |
| 4. Agent analyzes: Is this a bot? A glitch?       |
| 5. Agent generates JSON Incident Report           |
| 6. Sends report to FastAPI via WebSockets         |
+---------------------------------------------------+
       |
       v
[Live FastAPI Dashboard / Terminal] -> Human reads report & takes action
```

## 6. Implementation Phases

### Phase 1: The Data Simulator & Kafka (The Bloodstream)
*   **Goal:** Get live data flowing.
*   **Action:** Write a Python script that generates realistic JSON user events (e.g., `{"user_id": 123, "action": "login", "ip": "192.168.1.1", "timestamp": "..."}`). Include a "bot" mode that occasionally generates impossible speeds (e.g., 50 clicks in 1 second).
*   **Setup:** Spin up Kafka in Docker. The simulator publishes to the `raw-user-events` topic.

### Phase 2: The ML Detection Engine (The Brain)
*   **Goal:** Score the data in real-time.
*   **Action:** Write a Kafka consumer that reads the raw events. Use `scikit-learn`'s `IsolationForest`. 
*   **Logic:** Maintain a rolling window of user behavior. Extract features like "events in last 10 seconds" and "unique actions." Feed these to the model. If the model returns `-1` (anomaly), push the event to the `flagged-anomalies` topic.

### Phase 3: The Autonomous AI Agent (The Investigator)
*   **Goal:** Automated triage and reporting.
*   **Action:** Write a second Kafka consumer that reads the `flagged-anomalies` topic. This consumer triggers an AI Agent.
*   **Tools:** Give the Agent a Python function tool that can query a mock PostgreSQL database to get the user's account age and past 10 actions.
*   **Prompt:** *"You are a SOC analyst. An anomaly was detected. User ID 123 did X. Use your tools to get their history. Write a 2-paragraph incident report explaining the threat level and your recommendation (block, monitor, ignore)."*

### Phase 4: The Live Dashboard & Dockerization
*   **Goal:** Make it visual and one-click deployable.
*   **Action:** Build a simple FastAPI frontend with WebSockets. The AI Agent pushes its reports to the WebSocket, and they appear live on a webpage.
*   **Deploy:** Write a `docker-compose.yml` that spins up Kafka, PostgreSQL, the Simulator, the ML Consumer, and the API all at once.


