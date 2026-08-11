from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import json

app = FastAPI(title="Anomaly Engine Dashboard")

# Store active websocket connections
active_connections = []

# The HTML for the live dashboard
HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AIOps Security Center</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;800&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #0a0e14;
            --card-bg: #11151c;
            --border-color: #1f2630;
            --text-main: #e6e6e6;
            --text-muted: #8b98a9;
            --accent-blue: #00d4ff;
            --threat-high: #ff4757;
            --threat-medium: #ffa502;
            --threat-low: #2ed573;
        }
        body { 
            font-family: 'Inter', sans-serif; 
            background-color: var(--bg-color); 
            color: var(--text-main); 
            margin: 0; 
            padding: 40px; 
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 20px;
            margin-bottom: 30px;
        }
        .header h1 { 
            margin: 0; 
            font-size: 24px; 
            font-weight: 800;
            letter-spacing: -0.5px;
        }
        .header h1 span { color: var(--accent-blue); }
        .status-indicator {
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
            color: var(--threat-low);
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .status-dot {
            width: 8px;
            height: 8px;
            background-color: var(--threat-low);
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(46, 213, 115, 0.7); }
            70% { box-shadow: 0 0 0 10px rgba(46, 213, 115, 0); }
            100% { box-shadow: 0 0 0 0 rgba(46, 213, 115, 0); }
        }
        .report { 
            background-color: var(--card-bg); 
            border-left: 4px solid var(--threat-high); 
            padding: 20px; 
            margin-bottom: 15px; 
            border-radius: 4px; 
            border-top: 1px solid var(--border-color);
            border-right: 1px solid var(--border-color);
            border-bottom: 1px solid var(--border-color);
            animation: slideIn 0.3s ease-out;
        }
        @keyframes slideIn {
            from { opacity: 0; transform: translateY(-10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .report-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--border-color);
        }
        .target { 
            font-family: 'JetBrains Mono', monospace; 
            font-size: 14px; 
            color: var(--text-muted);
        }
        .target span { color: var(--text-main); font-weight: 700; }
        .badge { 
            font-size: 10px; 
            font-weight: 800; 
            text-transform: uppercase; 
            padding: 4px 8px; 
            border-radius: 4px; 
            font-family: 'JetBrains Mono', monospace;
        }
        .threat-high { color: var(--threat-high); border: 1px solid var(--threat-high); }
        .threat-medium { color: var(--threat-medium); border: 1px solid var(--threat-medium); }
        .threat-low { color: var(--threat-low); border: 1px solid var(--threat-low); }
        .report-body p { 
            margin: 8px 0; 
            font-size: 13px; 
            line-height: 1.5; 
        }
        .report-body strong { color: var(--text-muted); font-weight: 400; width: 150px; display: inline-block; font-family: 'JetBrains Mono', monospace;}
    </style>
</head>
<body>
    <div class="header">
        <h1>AIOps <span>Security</span> Operations Center</h1>
        <div class="status-indicator">
            <div class="status-dot"></div>
            LIVE STREAM ACTIVE
        </div>
    </div>
    <div id="reports"></div>
    <script>
        const ws = new WebSocket("ws://localhost:8000/ws");
        const reportsDiv = document.getElementById('reports');
        
        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            const div = document.createElement('div');
            const threatClass = 'threat-' + data.threat_level.toLowerCase();
            div.className = 'report ' + threatClass;
            
            div.innerHTML = `
                <div class="report-header">
                    <div class="target">TARGET USER: <span>${data.user_id}</span></div>
                    <div class="badge ${threatClass}">${data.threat_level} THREAT</div>
                </div>
                <div class="report-body">
                    <p><strong>ASSESSMENT:</strong> ${data.assessment}</p>
                    <p><strong>ACTION:</strong> ${data.recommendation}</p>
                </div>
            `;
            reportsDiv.prepend(div);
        };
    </script>
</body>
</html>
"""




@app.get("/")
async def get():
    return HTMLResponse(HTML)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)





#webhook ul fo fast api so that post request can broadcast to websocket
from pydantic import BaseModel

class AgentReport(BaseModel):
    user_id: int
    threat_level: str
    assessment: str
    recommendation: str

@app.post("/webhook")
async def receive_agent_report(report: AgentReport):
    # When the Agent POSTs here, broadcast to all connected WebSockets
    for connection in active_connections:
        await connection.send_text(report.json())
    return {"status": "received"}
