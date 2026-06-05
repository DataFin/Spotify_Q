# Spotify Data Platform — M1 Data & IA 2026

Plateforme de streaming musical distribuée inspirée de Spotify, construite de A à Z avec Apache Airflow, Kafka, Spark, Redis, PostgreSQL et MinIO.

---

## Architecture globale

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          SIMULATEUR P2P                                 │
│   python -m src.p2p_simulator.simulator --peers 10 --rate 3 --kafka    │
│           lpush → Redis queues    +    produce → Kafka topics           │
└──────────────────┬──────────────────────────────┬────────────────────────┘
                   │ PHASE 1 (Batch)              │ PHASE 2 (Streaming)
                   ▼                              ▼
┌──────────────────────────┐     ┌────────────────────────────────────────┐
│  Redis                   │     │  Apache Kafka (3 brokers KRaft)        │
│  listening_events_queue  │     │  6 topics — 6 partitions — RF=3        │
│  p2p_network_queue       │     │  9870+ messages dans listening_events  │
└──────────────┬───────────┘     └───────────────────┬────────────────────┘
               │                                     │
               ▼                                     ▼
┌──────────────────────────────┐  ┌─────────────────────────────────────────┐
│  streaming_events_pipeline   │  │  Spark Structured Streaming             │
│  (*/5 min)                   │  │  streaming_trends_job.py                │
│                              │  │  fenêtres tumbling 5 min                │
│  consume_from_redis          │  │  foreachBatch → PostgreSQL              │
│  validate → enrich           │  └───────────────────┬─────────────────────┘
│  upsert → PostgreSQL         │                      │
│  write → MinIO Parquet       │                      ▼
└──────────────┬───────────────┘  ┌─────────────────────────────────────────┐
               │                  │  PostgreSQL                             │
               ▼                  │  realtime_top_tracks                    │
┌──────────────────────────────┐  │  (mis à jour toutes les 5 min)          │
│  aggregation_pipeline        │  └─────────────────────────────────────────┘
│  (0 4 * * *)                 │
│  top 50 tracks → daily_streams│
│  stats artistes → artist_stats│
└──────────────┬───────────────┘
               ▼
┌──────────────────────────────┐
│  recommendation_pipeline     │
│  (0 5 * * *)                 │
│  collaborative filtering     │
│  Redis reco:{user_id} TTL 24h│
│  PostgreSQL recommendations  │
└──────────────────────────────┘

En parallèle :
┌──────────────────────────────────────────────────────────────────────────┐
│  dlq_reprocessing_pipeline (@hourly)                                     │
│  dead_letter_events (pending) → reprocessed / abandoned                 │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Stack technique

| Technologie | Version | Rôle |
|---|---|---|
| Apache Airflow | 2.9.1 | Orchestration des pipelines batch |
| PostgreSQL | 15 | Catalogue, événements, agrégats (11 tables) |
| Redis | 7 | Queue événements + cache recommandations |
| MinIO | latest | Stockage Parquet S3-compatible |
| Apache Kafka | 7.6.0 KRaft | Messaging temps réel (3 brokers) |
| Apache Spark | 3.5.0 | Structured Streaming |
| Python | 3.11 | Transformations, simulateur |
| Docker Compose | - | Stack locale complète |

---

## Structure du projet

```
Spotify_Q/
├── dags/                              # DAGs Airflow Phase 1
│   ├── catalog_ingestion_pipeline.py  # Ingestion catalogue MinIO → PostgreSQL
│   ├── streaming_events_pipeline.py   # Redis → PostgreSQL + MinIO
│   ├── aggregation_pipeline.py        # Agrégats quotidiens
│   ├── recommendation_pipeline.py     # Collaborative filtering
│   └── dlq_reprocessing_pipeline.py   # Retraitement DLQ
├── spark_jobs/                        # Jobs Spark Phase 2
│   └── streaming_trends_job.py        # Kafka → realtime_top_tracks
├── src/
│   ├── data_generator/
│   │   └── generate_catalog.py        # Générateur faker (3 labels)
│   ├── p2p_simulator/
│   │   └── simulator.py               # Simulateur P2P (Redis + Kafka)
│   └── transformations/
│       ├── catalog.py                 # normalize_artist_name, validate_track_schema
│       └── events.py                  # is_valid_listening_event
├── tests/
│   ├── structure/
│   │   └── test_dag_structure.py      # 16 tests de structure DAGs
│   └── unit/
│       └── test_transformations.py    # 18 tests unitaires → 34 PASSED
├── sql/
│   └── init_spotify_db.sql            # Schéma PostgreSQL (11 tables)
├── data/
│   └── labels/                        # JSONs générés (3 labels)
├── docs/
│   ├── DATA_MODEL.md                  # ERD + réponses aux questions
│   ├── ARCHITECTURE.md                # ETL/ELT, Kafka, Spark, leçons
│   ├── RUNBOOK.md                     # 3 incidents + procédures
│   └── GUIDE_COMPLET_PHASE1.md        # Guide validation Issues #1 à #9
├── docker-compose.yml                 # Stack complète unifiée
└── requirements.txt                   # Dépendances Python
```

---

## Démarrage rapide

```bash
# 1. Cloner le repo
git clone https://github.com/DataFin/Spotify_Q.git
cd Spotify_Q
git checkout batch-pipelines
git pull origin batch-pipelines

# 2. Créer l'environnement virtuel
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # Windows
source .venv/bin/activate       # Mac/Linux

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Lancer toute la stack Docker
docker compose up -d
docker compose ps  # tous doivent être Up

# 5. Générer et uploader le catalogue
python -m src.data_generator.generate_catalog --artists 15
python -c "
import boto3, glob, os
s3 = boto3.client('s3', endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
try: s3.create_bucket(Bucket='labels-raw')
except: pass
for f in glob.glob('data/labels/*.json'):
    s3.upload_file(f, 'labels-raw', os.path.basename(f))
    print('Uploadé:', f)
"

# 6. Lancer le simulateur Phase 1 (Redis uniquement)
python -m src.p2p_simulator.simulator --peers 10 --rate 3

# 7. Lancer le simulateur Phase 2 (Redis + Kafka)
python -m src.p2p_simulator.simulator --peers 10 --rate 3 --kafka

# 8. Trigger les DAGs Phase 1
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger catalog_ingestion_pipeline
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger streaming_events_pipeline

# 9. Lancer le job Spark Phase 2
# Préparer le cache Ivy (première fois uniquement)
docker exec --user root spotify_q-spark-master-1 mkdir -p /home/spark/.ivy2/cache
docker exec --user root spotify_q-spark-master-1 chown -R spark:spark /home/spark/.ivy2

# Lancer le job (sink console — valide la lecture Kafka)
docker exec spotify_q-spark-master-1 /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  /opt/spark-jobs/streaming_trends_job.py

# Les events JSON apparaissent dans les logs toutes les 10 secondes :
# docker logs spotify_q-spark-master-1 -f
```

---

## Interfaces

| Interface | URL | Identifiants |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO UI | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8090 | - |
| Spark Master UI | http://localhost:8888 | - |

---

## DAGs Phase 1

| DAG | Schedule | Rôle |
|---|---|---|
| `catalog_ingestion_pipeline` | `0 2 * * *` | Ingestion catalogue depuis MinIO |
| `streaming_events_pipeline` | `*/5 * * * *` | Collecte événements Redis → PostgreSQL + MinIO |
| `aggregation_pipeline` | `0 4 * * *` | Agrégats quotidiens top tracks + stats artistes |
| `recommendation_pipeline` | `0 5 * * *` | Recommandations collaborative filtering |
| `dlq_reprocessing_pipeline` | `@hourly` | Retraitement Dead Letter Queue |

---

## Topics Kafka Phase 2

| Topic | Partitions | Réplication | Description |
|---|---|---|---|
| `listening_events` | 6 | 3 | Events d'écoute principaux |
| `p2p_network_events` | 6 | 3 | Events réseau P2P |
| `catalog_updates` | 3 | 3 | Mises à jour catalogue (compaction) |
| `enriched_events` | 6 | 3 | Events enrichis |
| `fraud_alerts` | 3 | 3 | Alertes fraude |
| `late_listening_events` | 3 | 3 | Events en retard |

---

## Tests

```bash
# Lancer tous les tests
docker exec -it spotify_q-airflow-scheduler-1 bash -c \
  "cd /opt/airflow && python -m pytest tests/ -v --tb=short"

# Résultat : 34 passed, 0 failed, 0 skipped
```

---

## Validation Phase 1

```bash
# Vérifier toutes les tables PostgreSQL
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "
SELECT 'tracks' as table_name, COUNT(*) FROM tracks
UNION ALL SELECT 'artists', COUNT(*) FROM artists
UNION ALL SELECT 'listening_events', COUNT(*) FROM listening_events
UNION ALL SELECT 'daily_streams', COUNT(*) FROM daily_streams
UNION ALL SELECT 'artist_stats', COUNT(*) FROM artist_stats
UNION ALL SELECT 'recommendations', COUNT(*) FROM recommendations
UNION ALL SELECT 'dead_letter_events', COUNT(*) FROM dead_letter_events;"

# Vérifier Redis
docker exec -it spotify_q-redis-1 redis-cli -n 1 keys "reco:*"

# Vérifier MinIO
docker exec -it spotify_q-minio-1 mc ls local/spotify-parquet --recursive
```

## Validation Phase 2

```bash
# Vérifier les topics Kafka
docker exec spotify_q-kafka-1-1 kafka-topics --list --bootstrap-server kafka-1:9092

# Vérifier les messages
# http://localhost:8090 → Topics → listening_events → Message Count

# Vérifier realtime_top_tracks
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT * FROM realtime_top_tracks ORDER BY stream_count DESC LIMIT 5;"
```

---

## Données produites

| Table / Stockage | Résultat |
|---|---|
| `tracks` | 2 822 morceaux |
| `artists` | ~45 artistes |
| `listening_events` | 102 407+ écoutes |
| `daily_streams` | 50 tracks top |
| `recommendations` | 6 000 (600 users × 10) |
| `dead_letter_events` | 128 000+ (100 abandoned) |
| Redis `reco:*` | 600+ clés TTL 24h |
| MinIO Parquet | Partitionné date=/hour= |
| Kafka `listening_events` | 9 870+ messages |

---

## Problèmes connus et solutions

| Problème | Cause | Solution |
|---|---|---|
| `listening_events` vide | `redis.publish()` au lieu de `redis.lpush()` | Corriger `_publish_to_redis` dans `simulator.py` |
| `daily_streams` vide | `data_interval_start` au lieu de `data_interval_end` | Utiliser `context["data_interval_end"].date()` |
| Tous les events en DLQ `unknown_track` | Simulateur génère des UUIDs aléatoires | Charger les vrais `track_id` depuis PostgreSQL |
| FK violation `source_peer_id` | Table `peers` vide | Passer `None` pour `source_peer_id` |
| Transaction PostgreSQL aborted | `conn.rollback()` casse toute la transaction | Utiliser `SAVEPOINT sp` par INSERT |
| Kafka brokers ne démarrent pas | Mauvaise variable `KAFKA_CLUSTER_ID` | Utiliser `CLUSTER_ID` + UUID généré par `kafka-storage random-uuid` |
| `kafka-2:9094` inaccessible depuis Windows | Ports non exposés | Ajouter `ports: - "9094:9094"` dans docker-compose.yml |
| Spark S3A ClassNotFoundException | Image Spark sans hadoop-aws | Utiliser checkpoint local `/tmp/spark-checkpoints` |
