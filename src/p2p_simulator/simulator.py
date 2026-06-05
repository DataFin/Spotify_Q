"""
SPOTIFY — Simulateur P2P
========================
Ce simulateur génère des événements réalistes d'un réseau peer-to-peer
de streaming musical. Il publie dans Redis (Phase 1) ET dans Kafka (Phase 2).

Usage :
    python -m src.p2p_simulator.simulator --peers 10 --rate 5
    python -m src.p2p_simulator.simulator --kafka --peers 10 --rate 5
    python -m src.p2p_simulator.simulator --mode fraud --peers 5
"""

import argparse
import json
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timedelta

import redis

# Phase 2 — Kafka
try:
    from confluent_kafka import Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("p2p_simulator")


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

REDIS_URL        = "redis://localhost:6379/1"
KAFKA_BOOTSTRAP  = "localhost:9092"

TOPICS = {
    "listening":   "listening_events",
    "p2p_network": "p2p_network_events",
}

DEVICE_TYPES  = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]


# ─────────────────────────────────────────────────────────────
# SIMULATEUR PRINCIPAL
# ─────────────────────────────────────────────────────────────

class P2PSimulator:

    def __init__(
        self,
        n_peers: int = 10,
        events_per_second: float = 5.0,
        mode: str = "normal",
        enable_kafka: bool = False,
    ):
        self.n_peers           = n_peers
        self.events_per_second = events_per_second
        self.mode              = mode
        self.enable_kafka      = enable_kafka and KAFKA_AVAILABLE
        self.running           = True
        self.event_count       = 0

        # Connexion Redis
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)

        # Kafka Producer (Phase 2)
        self.kafka_producer = None
        if self.enable_kafka:
            self.kafka_producer = Producer({
                "bootstrap.servers":  KAFKA_BOOTSTRAP,
                "acks":               "all",
                "enable.idempotence": True,
                "retries":            3,
                "retry.backoff.ms":   500,
            })
            logger.info("Kafka producer initialisé")

        # Charger les vrais track_id depuis PostgreSQL
        self.track_ids = self._load_track_ids()
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]
        self.sample_users = [str(uuid.uuid4()) for _ in range(200)]

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        mode_str = "normal" if not enable_kafka else "normal+kafka"
        logger.info(
            f"Simulateur démarré | mode={mode_str} | peers={n_peers} | rate={events_per_second} evt/s"
        )

    def _load_track_ids(self) -> list:
        """
        Charge les vrais track_id depuis PostgreSQL.
        Fallback sur des UUIDs aléatoires si PostgreSQL est indisponible.
        """
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                dbname="spotify",
                user="spotify",
                password="spotify",
            )
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM tracks")
            ids = [str(row[0]) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            if ids:
                logger.info(f"Chargé {len(ids)} tracks depuis PostgreSQL")
                return ids
        except Exception as e:
            logger.warning(f"PostgreSQL indisponible, utilisation de tracks aléatoires : {e}")

        # Fallback
        return [str(uuid.uuid4()) for _ in range(50)]

    def run(self):
        """Boucle principale : génère et publie des événements en continu."""
        interval = 1.0 / self.events_per_second

        while self.running:
            try:
                if random.random() < 0.8:
                    event = self._generate_listening_event()
                    self._publish_event("listening", event)
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event("p2p_network", event)

                self.event_count += 1

                if self.event_count % 100 == 0:
                    logger.info(f"Événements publiés : {self.event_count}")

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Erreur lors de la génération d'événement : {e}")
                time.sleep(1)

        # Flush Kafka avant d'arrêter
        if self.kafka_producer:
            self.kafka_producer.flush(timeout=10)

    # ── Génération d'événements ──────────────────────────────

    def _generate_listening_event(self) -> dict:
        track_id    = random.choice(self.track_ids)
        duration_ms = random.randint(30_000, 300_000)
        completed   = duration_ms >= 30_000

        event = {
            "event_id":     str(uuid.uuid4()),
            "user_id":      random.choice(self.sample_users),
            "track_id":     track_id,
            "source_peer":  random.choice(self.active_peers),
            "timestamp":    datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
                            if hasattr(datetime, "UTC")
                            else datetime.utcnow().isoformat() + "Z",
            "duration_ms":  duration_ms,
            "device_type":  random.choice(DEVICE_TYPES),
            "geo_country":  random.choice(GEO_COUNTRIES),
            "completed":    completed,
            "event_source": random.choice(EVENT_SOURCES),
        }

        # Mode fraud
        if self.mode == "fraud" and random.random() < 0.3:
            event["duration_ms"] = random.randint(100, 4999)
            event["completed"]   = False

        # Mode late_events
        if self.mode == "late_events" and random.random() < 0.4:
            delay_minutes    = random.randint(5, 30)
            ts               = datetime.utcnow() - timedelta(minutes=delay_minutes)
            event["timestamp"] = ts.isoformat() + "Z"

        return event

    def _generate_p2p_network_event(self) -> dict:
        event_type = random.choice([
            "peer_connect", "peer_disconnect",
            "chunk_transfer", "cache_hit", "cache_miss",
        ])
        peer_id = random.choice(self.active_peers)

        event = {
            "event_id":   str(uuid.uuid4()),
            "event_type": event_type,
            "peer_id":    peer_id,
            "timestamp":  datetime.utcnow().isoformat() + "Z",
        }

        if event_type == "peer_connect":
            event["ip_address"] = f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
            event["latency_ms"] = random.randint(5, 200)
        elif event_type == "peer_disconnect":
            event["reason"]             = random.choice(["timeout", "user_quit", "network_error", "graceful"])
            event["session_duration_s"] = random.randint(30, 3600)
        elif event_type == "chunk_transfer":
            event["from_peer"]   = random.choice(self.active_peers)
            event["to_peer"]     = peer_id
            event["chunk_size"]  = random.randint(64_000, 512_000)
            event["track_id"]    = random.choice(self.track_ids)
            event["success"]     = random.random() > 0.05
        elif event_type == "cache_hit":
            event["track_id"]    = random.choice(self.track_ids)
            event["saved_bytes"] = random.randint(64_000, 512_000)
        elif event_type == "cache_miss":
            event["track_id"]      = random.choice(self.track_ids)
            event["fallback_peer"] = random.choice(self.active_peers)

        return event

    # ── Publication ──────────────────────────────────────────

    def _publish_event(self, topic_key: str, event: dict):
        """Publie un événement dans Redis ET Kafka (si activé)."""
        payload = json.dumps(event)
        channel = TOPICS[topic_key]

        # Phase 1 — Redis (toujours actif)
        self._publish_to_redis(channel, payload)

        # Phase 2 — Kafka (si activé)
        if self.enable_kafka and self.kafka_producer:
            self._publish_to_kafka(channel, event.get("user_id", ""), payload)

    def _publish_to_redis(self, channel: str, payload: str):
        """Publie dans la queue Redis via lpush."""
        try:
            self.redis.lpush(channel + "_queue", payload)
        except redis.RedisError as e:
            logger.error(f"Redis indisponible — channel={channel} | erreur={e}")

    def _publish_to_kafka(self, topic: str, key: str, payload: str):
        """
        Publie dans le topic Kafka.
        - acks=all + enable.idempotence=True → exactly-once
        - key = user_id pour le partitionnement cohérent
        """
        def delivery_report(err, msg):
            if err:
                logger.error(f"Kafka delivery failed — topic={msg.topic()} | err={err}")

        try:
            self.kafka_producer.produce(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )
            self.kafka_producer.poll(0)  # déclenche les callbacks
        except Exception as e:
            logger.error(f"Kafka indisponible — topic={topic} | erreur={e}")

    def _shutdown(self, signum, frame):
        logger.info(
            f"Arrêt du simulateur (signal {signum}) — {self.event_count} événements publiés"
        )
        self.running = False


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SPOTIFY P2P Simulator")
    parser.add_argument("--peers",  type=int,   default=10,      help="Nombre de peers simulés")
    parser.add_argument("--rate",   type=float, default=5.0,     help="Événements par seconde")
    parser.add_argument("--mode",   type=str,   default="normal",
                        choices=["normal", "fraud", "late_events", "chaos"])
    parser.add_argument("--kafka",  action="store_true",         help="Activer la publication Kafka")
    args = parser.parse_args()

    simulator = P2PSimulator(
        n_peers=args.peers,
        events_per_second=args.rate,
        mode=args.mode,
        enable_kafka=args.kafka,
    )
    simulator.run()


if __name__ == "__main__":
    main()
