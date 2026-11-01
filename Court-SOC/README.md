# 🛡️ Court-SOC (Cyber Sentinel Dedicated SOC Server)

Dedicated Security Operations Center (SOC) dashboard and live telemetry microservice for JSM Chambers.

---

## 📦 What is inside this project:
- **`index.html` & `security-dashboard.html`**: Cyber Sentinel SOC Dashboard with live heartbeat monitoring of Main Server A (detects server crashes and displays real-time outage alerts).
- **`app.py`**: Lightweight SOC backend serving telemetry, live Server A health probes, database health monitoring (MongoDB Atlas, Turso 1, Turso 2), and WAF defense tables.
- **`vercel.json`**: Pre-configured Vercel deployment specification for Court-SOC.
- **`requirements.txt`**: Minimal Python dependencies.
- **`css/`** & **`js/`**: Styling and asset dependencies.

---

## 🚀 How to Run Locally:
```bash
cd Court-SOC
python3 app.py
```
Open: `http://localhost:8082`

---

## 🚀 How to Deploy as an Independent Vercel Project:
1. Initialize a separate GitHub repository for Court-SOC (e.g. `court-soc`).
2. Push only the contents of this `Court-SOC` directory to that repository.
3. Import the repository in Vercel as a new project.
4. Set the Environment Variables:
   - `MAIN_SERVER_URL` = `https://jsm-chamber-cuttack-odisha-30.vercel.app`
   - `MONGO_URI`
   - `DB_NAME` = `jsmchambers_db`
   - `TURSO_DATABASE_URL`
   - `TURSO_AUTH_TOKEN`
   - `TURSO_CONTENT_DATABASE_URL`
   - `TURSO_CONTENT_AUTH_TOKEN`
