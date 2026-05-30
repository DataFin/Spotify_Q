"""
SPOTIFY Dashboard — Serveur FastAPI
Expose les données PostgreSQL et Redis pour le dashboard visuel
"""
import json
import random
from datetime import datetime, timezone, timedelta

try:
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import HTMLResponse
    import psycopg2
    import redis as redis_lib
except ImportError:
    pass

app = FastAPI(title="SPOTIFY Demo Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_mock_data():
    """Génère des données simulées si PostgreSQL/Redis non disponibles"""
    genres = ["Pop", "Hip-Hop", "Electronic", "Rock", "R&B", "Jazz", "Latin", "Metal"]
    artists = ["The Weeknd", "Daft Punk", "Billie Eilish", "Drake", "Beyoncé", 
               "Kendrick Lamar", "Taylor Swift", "Bad Bunny", "Rosalía", "Frank Ocean"]
    
    tracks = []
    for i in range(20):
        tracks.append({
            "rank": i + 1,
            "title": f"Track {random.randint(1,999):03d}",
            "artist": random.choice(artists),
            "genre": random.choice(genres),
            "streams": random.randint(50000, 2000000),
            "duration_ms": random.randint(120000, 300000),
        })
    tracks.sort(key=lambda x: x["streams"], reverse=True)
    
    return {
        "top_tracks": tracks[:10],
        "stats": {
            "total_streams": random.randint(5000000, 50000000),
            "active_peers": random.randint(8000, 25000),
            "active_listeners": random.randint(500000, 2000000),
            "fraud_alerts": random.randint(0, 47),
            "cache_hit_rate": round(random.uniform(0.65, 0.92), 2),
            "avg_latency_ms": random.randint(8, 45),
        },
        "genre_distribution": [
            {"genre": g, "streams": random.randint(100000, 5000000)} 
            for g in genres
        ],
        "p2p_events_per_min": random.randint(800, 3000),
        "pipeline_status": {
            "catalog_ingestion": "success",
            "streaming_events": "running",
            "aggregation": "success",
            "recommendation": "success",
            "dlq_reprocessing": "success",
        }
    }

@app.get("/api/dashboard")
async def get_dashboard():
    return get_mock_data()

@app.get("/api/live-events")
async def get_live_events():
    events = []
    countries = ["FR", "US", "DE", "GB", "BR", "JP", "ES"]
    devices = ["mobile", "desktop", "smart_speaker", "web"]
    for _ in range(10):
        events.append({
            "user": f"user_{random.randint(1000,9999)}",
            "track": f"Track {random.randint(1,999):03d}",
            "country": random.choice(countries),
            "device": random.choice(devices),
            "peer": f"peer_{random.randint(100,999)}",
            "ts": datetime.now(timezone.utc).isoformat(),
        })
    return events

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
