# Spotify Data Platform — M1 Data & IA 2026

Plateforme de streaming musical distribuée construite avec Apache Airflow, Redis, PostgreSQL et MinIO.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SIMULATEUR P2P                               │
│          python -m src.p2p_simulator.simulator                      │
│                    lpush → Redis queues                             │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│              streaming_events_pipeline  (*/5 min)                   │
│                                                                     │
│  consume_from_redis                                                 │
│       ├── validate_listening → enrich_events → store_to_parquet    │
│       │                                    → upsert_to_postgres    │
│       └── validate_p2p → store_p2p_to_parquet                      │
│                                                                     │
│  Redis → PostgreSQL (listening_events) + MinIO (Parquet)           │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│              aggregation_pipeline  (0 4 * * *)                      │
│                                                                     │
│  compute_top_tracks + compute_artist_stats + compute_p2p_metrics   │
│                    → update_aggregates                              │
│                                                                     │
│  PostgreSQL → daily_streams + artist_stats                         │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│              recommendation_pipeline  (0 5 * * *)                   │
│                                                                     │
│  build_user_track_matrix → compute_recommendations                 │
│                         → store_recommendations                    │
│                                                                     │
│  PostgreSQL → Redis (reco:{user_id}) + recommendations             │
└─────────────────────────────────────────────────────────────────────┘

En parallèle :
┌─────────────────────────────────────────────────────────────────────┐
│              dlq_reprocessing_pipeline  (@hourly)                   │
│  dead_letter_events (pending) → reprocessed / abandoned            │
└─────────────────────────────────────────────────────────────────────┘
```

## Stack technique

| Technologie | Version | Rôle |
|---|---|---|
| Apache Airflow | 2.9.1 | Orchestration des pipelines |
| PostgreSQL | 15 | Catalogue, événements, agrégats |
| Redis | 7 | Queue événements + cache recommandations |
| MinIO | latest | Stockage Parquet (S3-compatible) |
| Python | 3.11 | Transformations, simulateur |
| Docker Compose | - | Stack locale complète |

## Démarrage rapide

```bash
# 1. Cloner le repo
git clone https://github.com/DataFin/Spotify_Q.git
cd Spotify_Q
git checkout batch-pipelines
git pull origin batch-pipelines

# 2. Créer l'environnement virtuel
python -m venv .venv
source .venv/bin/activate  # Mac/Linux
.\.venv\Scripts\Activate.ps1  # Windows

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Lancer la stack Docker
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

# 6. Lancer le simulateur P2P (terminal dédié)
python -m src.p2p_simulator.simulator --peers 10 --rate 3

# 7. Trigger les DAGs
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger catalog_ingestion_pipeline
# Attendre 1 minute
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger streaming_events_pipeline
```

## Interfaces

| Interface | URL | Identifiants |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO UI | http://localhost:9001 | minioadmin / minioadmin |

## DAGs

| DAG | Schedule | Rôle |
|---|---|---|
| `catalog_ingestion_pipeline` | `0 2 * * *` | Ingestion catalogue depuis MinIO |
| `streaming_events_pipeline` | `*/5 * * * *` | Collecte événements Redis |
| `aggregation_pipeline` | `0 4 * * *` | Agrégats quotidiens |
| `recommendation_pipeline` | `0 5 * * *` | Recommandations collaborative filtering |
| `dlq_reprocessing_pipeline` | `@hourly` | Retraitement Dead Letter Queue |

## Tests

```bash
# Lancer tous les tests
docker exec -it spotify_q-airflow-scheduler-1 bash -c \
  "cd /opt/airflow && python -m pytest tests/ -v --tb=short"

# Résultat attendu : 34 passed, 0 failed
```

## Validation Phase 1

```bash
# Vérifier toutes les tables
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "
SELECT 'tracks' as table_name, COUNT(*) FROM tracks
UNION ALL SELECT 'listening_events', COUNT(*) FROM listening_events
UNION ALL SELECT 'daily_streams', COUNT(*) FROM daily_streams
UNION ALL SELECT 'recommendations', COUNT(*) FROM recommendations
UNION ALL SELECT 'dead_letter_events', COUNT(*) FROM dead_letter_events;"

# Vérifier Redis
docker exec -it spotify_q-redis-1 redis-cli -n 1 keys "reco:*"

# Vérifier MinIO
docker exec -it spotify_q-minio-1 mc ls local/spotify-parquet --recursive
```

## Documentation

- `docs/DATA_MODEL.md` — ERD + modèle de données
- `docs/ARCHITECTURE.md` — Choix ETL/ELT par pipeline
- `docs/RUNBOOK.md` — Procédures d'incidents
- `docs/GUIDE_COMPLET_PHASE1.md` — Guide de validation Phase 1

## Problèmes connus

| Problème | Cause | Solution |
|---|---|---|
| `listening_events` vide | Simulateur utilise `publish` au lieu de `lpush` | Vérifier `simulator.py` ligne `_publish_to_redis` |
| `daily_streams` vide | Mauvaise date (`data_interval_start` vs `data_interval_end`) | Utiliser `data_interval_end.date()` |
| Tous les events en DLQ `unknown_track` | Simulateur génère des UUIDs aléatoires | Récupérer la version corrigée depuis `batch-pipelines` |
| FK violation `source_peer_id` | Table `peers` vide | Passer `None` pour `source_peer_id` |
