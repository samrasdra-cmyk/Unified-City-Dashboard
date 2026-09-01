
---

## 📄 README.md 

```markdown
# 🌆 Unified City Dashboard

[![Live Demo](https://img.shields.io/badge/Live-Demo-3fb950?style=for-the-badge&logo=vercel)](https://unified-city-dashboard-vruo.vercel.app/)
[![API Docs](https://img.shields.io/badge/API-Docs-0077ff?style=for-the-badge&logo=swagger)](https://unified-city-dashboard.onrender.com/docs)
[![GitHub](https://img.shields.io/badge/GitHub-Repo-181717?style=for-the-badge&logo=github)](https://github.com/samrasdra-cmyk/Unified-City-Dashboard)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](LICENSE)

> **Real‑time urban intelligence platform that unifies traffic, air quality, transit, waste, and thermal data with ML‑powered forecasting.**

---

## 🎯 Overview

Cities generate massive amounts of data, but it’s often fragmented – traffic on one app, air quality on another, transit and waste invisible to the public.  
**Unified City Dashboard** brings everything together into a single, real‑time, interactive view, helping citizens and city planners make better decisions.

---

## ✨ Features

| Domain | Description | Data Source |
|--------|-------------|-------------|
| 🚦 **Traffic** | Live speed, congestion, and flow | TomTom Traffic API |
| 🌫️ **Air Quality** | AQI, PM2.5, weather conditions | OpenWeatherMap |
| 🚌 **Transit** | Vehicle positions & on‑time performance | GTFS‑RT Feed |
| 🗑️ **Waste** | Bin fill levels and collection insights | IoT / smart pattern model |
| 🌡️ **Thermal Comfort** | Feels‑like temperature, heat index | FortyGuard Temperature API |
| 📊 **ML Forecasting** | Congestion predictions (RandomForest) | scikit‑learn |
| 🗺️ **Interactive Map** | Heatmaps, markers, layers | MapLibre + OpenStreetMap |
| 📈 **Historical Trends** | 6‑hour charts & time‑series | PostgreSQL/TimescaleDB |
| 🔄 **Live Updates** | Real‑time WebSocket streaming | FastAPI WebSocket |

---

## 🧠 Machine Learning

A **RandomForestRegressor** model predicts a congestion index (0–1) for five urban zones using:

- Time of day & day of week
- Weekend indicator
- Current temperature & weather severity
- Population increase factor

The output is served as GeoJSON, allowing planners to visualise future congestion patterns.

---

## 🏗️ Architecture

```
  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
  │   TomTom    │  │OpenWeather  │  │  GTFS‑RT    │  │    IoT      │  │ FortyGuard  │
  │   Traffic   │  │    Air      │  │   Transit   │  │   Waste     │  │ Temperature │
  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
         │                │                │                │                │
         └────────────────┴────────────────┴────────────────┴────────────────┘
                                          │
                                          ▼
                               ┌─────────────────────┐
                               │   Apache Kafka      │  ← Real‑time streaming
                               │   (Aiven – free)    │
                               └──────────┬──────────┘
                                          │
                 ┌────────────────────────┼────────────────────────┐
                 │                        │                        │
                 ▼                        ▼                        ▼
        ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
        │   PostgreSQL    │  │   TimescaleDB   │  │      Redis      │
        │  (Historical)   │  │ (Time‑series)   │  │  (Live cache)   │
        │  (Neon – free)  │  │  (Neon – free)  │  │ (Upstash – free)│
        └────────┬────────┘  └────────┬────────┘  └────────┬────────┘
                 │                    │                    │
                 └────────────────────┼────────────────────┘
                                      ▼
                             ┌─────────────────────┐
                             │      FastAPI        │  ← REST + WebSocket
                             │      (Render)       │
                             └──────────┬──────────┘
                                        │
                                        ▼
                             ┌─────────────────────┐
                             │   React + Vite      │  ← Frontend
                             │   (Vercel)          │
                             └─────────────────────┘
```

---

## 🛠️ Tech Stack

### Frontend
- React 18, TypeScript, Vite  
- MapLibre GL (interactive maps)  
- ECharts (data visualisation)  
- WebSocket (real‑time updates)

### Backend
- FastAPI (REST + WebSocket)  
- Python 3.11  
- Apache Kafka (streaming)  
- PostgreSQL / TimescaleDB (history)  
- Redis (live cache)  
- scikit‑learn (ML)

### Deployment (All Free Tiers)
- **Vercel** – Frontend  
- **Render** – Backend API  
- **Aiven** – Apache Kafka  
- **Neon** – PostgreSQL  
- **Upstash** – Redis

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Node.js 18+
- Docker (optional, for local Kafka)
- API keys (TomTom, OpenWeather, FortyGuard, GTFS‑RT)

### Clone & Setup
```bash
git clone https://github.com/samrasdra-cmyk/Unified-City-Dashboard.git
cd Unified-City-Dashboard
```

### Backend
```bash
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Fill in your API keys and service URLs
```

### Frontend
```bash
cd frontend
npm install
cp .env.example .env
# Set VITE_API_BASE_URL to your backend URL
```

### Run Locally
- **Backend:** `uvicorn app.main:app --reload --port 8000` (from `backend/`)
- **Frontend:** `npm run dev` (from `frontend/`)
- Open `http://localhost:5173`

---

## 🔑 Environment Variables

### Backend (`.env`)
```env
# APIs
TOMTOM_API_KEY=your_key
OPENWEATHER_API_KEY=your_key
FORTYGUARD_API_KEY=your_key
GTFS_RT_URL=your_gtfs_url

# Kafka (Aiven)
KAFKA_BOOTSTRAP_SERVERS=your_broker:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_USERNAME=your_username
KAFKA_PASSWORD=your_password
KAFKA_SSL_CA_LOCATION=/etc/ssl/certs/ca-certificates.crt

# Databases
DATABASE_URL=postgresql://user:pass@host:5432/db
REDIS_URL=redis://:password@host:6379

# Adapter
API_ADAPTER_ENABLED=true
CORS_ORIGINS=https://your-frontend.vercel.app
```

### Frontend (`.env`)
```env
VITE_API_BASE_URL=https://your-backend.onrender.com
VITE_WS_URL=wss://your-backend.onrender.com/ws/live
```

---

## 🔌 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/dashboard/latest` | Latest aggregated KPIs |
| GET | `/api/v1/dashboard/heatmap?type=` | Heatmap GeoJSON (traffic, temperature, etc.) |
| GET | `/api/v1/dashboard/history?metric=` | 6‑hour time‑series data |
| POST | `/api/v1/dashboard/simulate/population` | ML congestion forecast |
| GET | `/api/v1/admin/connectors/status` | Circuit‑breaker health for each data source |
| WS | `/ws/live` | Live WebSocket stream |
| GET | `/health` | Health check |

📘 **Swagger UI:** https://unified-city-dashboard.onrender.com/docs

---

## 🗺️ Map Integration

The dashboard uses **MapLibre GL** with **OpenStreetMap** raster tiles:
- Free, no API key required
- Fast and reliable
- Supports heatmaps, markers, and custom layers

---

## 📂 Project Structure

```
unified-city-dashboard/
├── backend/
│   ├── app/
│   │   ├── api/          → REST & WebSocket endpoints
│   │   ├── core/         → Config, DB, Kafka, Redis clients
│   │   ├── models/       → Data models
│   │   ├── services/     → Adapters, producers, consumers
│   │   └── main.py       → Entry point
│   ├── alembic/          → DB migrations
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   ├── src/
│   │   ├── components/   → React components
│   │   ├── hooks/        → Custom hooks (WebSocket)
│   │   ├── services/     → API client
│   │   ├── types/        → TypeScript definitions
│   │   └── App.tsx
│   ├── package.json
│   └── vite.config.ts
├── simulator/
│   └── data_simulator.py → Fallback data generator
├── docker-compose.yml
└── README.md
```

---

## 🧪 Testing

```bash
# Backend
cd backend
pytest

# Frontend
cd frontend
npm run test
```

---

## 🤝 Contributing

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push (`git push origin feature/amazing`)
5. Open a Pull Request

---

## 📜 License

MIT License – see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgements

- [FortyGuard](https://fortyguard.com) – Temperature API®
- [TomTom](https://developer.tomtom.com) – Traffic API
- [OpenWeatherMap](https://openweathermap.org) – Air Quality API
- [MapLibre](https://maplibre.org) – Open‑source maps
- [OpenStreetMap](https://openstreetmap.org) – Map data
- [Aiven](https://aiven.io), [Neon](https://neon.tech), [Upstash](https://upstash.com) – Free hosting
- [Vercel](https://vercel.com) & [Render](https://render.com) – Deployment

---

## 📞 Contact

- **Email:** samrasdra@gmail.com
- **GitHub:** [samrasdra-cmyk](https://github.com/samrasdra-cmyk)
- **Live Demo:** https://unified-city-dashboard-vruo.vercel.app/

---

**Built with ❤️ for smarter, cooler, more resilient cities.**


---

Just copy the content above into your `README.md` file, commit, and push. Done! 🚀
