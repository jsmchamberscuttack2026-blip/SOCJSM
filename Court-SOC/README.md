# 🛡️ Court-SOC (Cyber Sentinel Dedicated SOC Server)

Standalone, air-gapped Security Operations Center (SOC) dashboard and live telemetry microservice for JSM Chambers (High Court of Orissa).

---

## 📦 What is inside this project:
- **`index.html`**: Cyber Sentinel SOC Dashboard with live heartbeat monitoring of Main Server A (detects server crashes and displays real-time outage alerts).
- **`app.py`**: Lightweight SOC backend serving telemetry, live Server A health probes, database health monitoring (MongoDB Atlas, Turso 1, Turso 2), and WAF defense tables.
- **`vercel.json`**: Pre-configured Vercel deployment specification.
- **`requirements.txt`**: Minimal, lightweight Python dependencies.
- **`css/`** & **`js/`**: Styling and asset dependencies.
- **`chamber-owner.jpg`**: Dashboard branding and favicon.

---

## 🚀 How to Push to GitHub & Deploy on Vercel:

### Step 1: Push to a New GitHub Repository
Open your terminal in this `Court-SOC` folder:
```bash
git init
git add .
git commit -m "Initial commit for Cyber Sentinel Dedicated SOC"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/court-soc.git
git push -u origin main
```

### Step 2: Deploy on Vercel
1. Go to your **Vercel Dashboard**.
2. Click **"Add New..." ➔ "Project"**.
3. Import the **`court-soc`** GitHub repository.
4. Under **Environment Variables**, add:
   - `MAIN_SERVER_URL` = `https://jsm-chamber-cuttack-odisha-30.vercel.app`
   - `MONGO_URI`
   - `TURSO_DATABASE_URL`
   - `TURSO_AUTH_TOKEN`
   - `TURSO_CONTENT_DATABASE_URL`
   - `TURSO_CONTENT_AUTH_TOKEN`
5. Click **Deploy**.
6. (Optional) In Vercel Project Settings ➔ **Domains**, attach your subdomain: `soc.yourchamber.com`.
