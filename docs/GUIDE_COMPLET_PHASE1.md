# Guide complet Phase 1 — Spotify Data Platform

## Ordre d'exécution obligatoire

```
Issue #1 — Setup Docker
Issue #2 — Schéma PostgreSQL + Documentation
Issue #3 — Générateur de données + Upload MinIO
Issue #4 — DAG catalog_ingestion_pipeline
Issue #5 — Simulateur P2P
Issue #6 — DAG streaming_events_pipeline
Issue #7 — DAG aggregation_pipeline
Issue #8 — DAG recommendation_pipeline
Issue #9 — DAG dlq_reprocessing_pipeline
```

---

## Issue #1 — Setup Docker

```bash
# Cloner le repo
git clone https://github.com/DataFin/Spotify_Q.git
cd Spotify_Q

# Récupérer la branche de travail
git fetch --all
git checkout batch-pipelines
git pull origin batch-pipelines

# Lancer la stack
cp .env.example .env
docker compose up -d
docker compose ps  # tous doivent être Up ou healthy
```

**Interfaces disponibles :**
- Airflow UI : http://localhost:8080 (admin / admin)
- MinIO UI : http://localhost:9001 (minioadmin / minioadmin)

---

## Issue #2 — Schéma PostgreSQL + Documentation

```bash
# Vérifier que les tables existent
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "\dt"
# Résultat attendu : 11 tables
```

Les fichiers de documentation sont dans `docs/` :
- `docs/DATA_MODEL.md` — ERD + réponses aux questions
- `docs/ARCHITECTURE.md` — choix ETL/ELT par pipeline

---

## Issue #3 — Générateur de données + Upload MinIO

```bash
# Créer l'environnement virtuel
python -m venv .venv

# Windows
.\.venv\Scripts\Activate.ps1
# Mac/Linux
source .venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt
# Si pandas échoue (Python 3.14) :
pip install redis faker boto3 pandas pyarrow psycopg2-binary --only-binary=:all:

# Générer les catalogues
python -m src.data_generator.generate_catalog --artists 15

# Vérifier les fichiers générés
ls data/labels/
# Résultat attendu : sunset_records.json, nightwave_music.json, urban_pulse.json

# Uploader dans MinIO (port 9000 = API, port 9001 = UI)
python -c "
import boto3, glob, os
s3 = boto3.client('s3', endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
try:
    s3.create_bucket(Bucket='labels-raw')
except: pass
for f in glob.glob('data/labels/*.json'):
    s3.upload_file(f, 'labels-raw', os.path.basename(f))
    print('Uploadé:', f)
"

# Lancer les tests
python -m pytest tests/unit/test_transformations.py::TestDataGenerator -v
# Résultat attendu : 4 tests PASSED
```

---

## Issue #4 — DAG catalog_ingestion_pipeline

```bash
# Trigger le DAG
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger catalog_ingestion_pipeline

# Attendre 1 minute puis vérifier
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM tracks;"
# Résultat attendu : ~1400 tracks

docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM artists;"
# Résultat attendu : ~45 artistes

# Vérifier l'idempotence — relancer 2 fois, le COUNT doit rester identique
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger catalog_ingestion_pipeline

# Lancer les tests de structure
docker exec -it spotify_q-airflow-scheduler-1 bash -c \
  "cd /opt/airflow && python -m pytest tests/structure/test_dag_structure.py::TestCatalogIngestionDAG -v"
# Résultat attendu : 7 tests PASSED
```

---

## Issue #5 — Simulateur P2P

```bash
# Lancer le simulateur dans un terminal dédié (laisser tourner)
python -m src.p2p_simulator.simulator --peers 10 --rate 3

# Vérifier que les events arrivent dans Redis
docker exec -it spotify_q-redis-1 redis-cli -n 1 llen listening_events_queue
# Résultat attendu : nombre croissant
```

---

## Issue #6 — DAG streaming_events_pipeline

### Prérequis
- Issue #4 validée (catalogue chargé)
- Issue #5 active (simulateur qui tourne)

```bash
# Trigger le DAG
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger streaming_events_pipeline

# Attendre 1 minute puis valider les 3 critères

# Critère 1 — PostgreSQL COUNT(*) > 0
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM listening_events;"

# Critère 2 — Fichiers Parquet dans MinIO
docker exec -it spotify_q-minio-1 mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec -it spotify_q-minio-1 mc ls local/spotify-parquet --recursive

# Critère 3 — DLQ active
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT error_type, COUNT(*) FROM dead_letter_events GROUP BY error_type;"
```

### Points d'attention
- Le simulateur doit tourner AVANT de trigger le DAG
- Si COUNT(*) = 0, vérifier les logs :
```bash
docker logs spotify_q-airflow-worker-1 --tail 50 2>&1 | grep "WARNING\|ERROR\|inserted"
```

---

## Issue #7 — DAG aggregation_pipeline

### Prérequis
- Issue #6 validée avec données dans `listening_events`

```bash
# Trigger le DAG
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger aggregation_pipeline

# Attendre 2 minutes puis valider

# Critère principal
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT * FROM daily_streams ORDER BY total_streams DESC LIMIT 10;"
# Résultat attendu : 10 lignes avec total_streams, unique_listeners, countries

# Vérification complémentaire
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM artist_stats;"
```

### Points d'attention
- Si daily_streams est vide, vérifier la date des données :
```bash
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT DATE(timestamp), COUNT(*) FROM listening_events GROUP BY DATE(timestamp);"
```

---

## Issue #8 — DAG recommendation_pipeline

### Prérequis
- Issue #7 validée

```bash
# Trigger le DAG
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger recommendation_pipeline

# Attendre 2 minutes puis valider

# Critère 1 — Clés Redis
docker exec -it spotify_q-redis-1 redis-cli -n 1 keys "reco:*"
# Résultat attendu : 500+ clés

# Critère 2 — Contenu d'une clé (remplacer <user_id> par une clé de la liste)
docker exec -it spotify_q-redis-1 redis-cli -n 1 get "reco:<user_id>"
# Résultat attendu : liste JSON de 10 track_ids

# Critère 3 — PostgreSQL
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM recommendations;"
# Résultat attendu : count >= 5000
```

---

## Issue #9 — DAG dlq_reprocessing_pipeline

```bash
# Injecter un event de test
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "INSERT INTO dead_letter_events (payload, error_type) VALUES ('{}', 'test');"

# Trigger le DAG
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger dlq_reprocessing_pipeline

# Attendre 1 minute puis vérifier les transitions de statut
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT status, COUNT(*) FROM dead_letter_events GROUP BY status ORDER BY status;"
# Résultat attendu : transitions pending → reprocessed / abandoned
```

---

## Validation globale Phase 1

```bash
# Tous les DAGs doivent être verts
# http://localhost:8080

# Lancer tous les tests de structure
docker exec -it spotify_q-airflow-scheduler-1 bash -c \
  "cd /opt/airflow && python -m pytest tests/structure/ -v"

# Résumé des données attendues
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "
SELECT 'tracks' as table_name, COUNT(*) FROM tracks
UNION ALL SELECT 'artists', COUNT(*) FROM artists
UNION ALL SELECT 'listening_events', COUNT(*) FROM listening_events
UNION ALL SELECT 'daily_streams', COUNT(*) FROM daily_streams
UNION ALL SELECT 'artist_stats', COUNT(*) FROM artist_stats
UNION ALL SELECT 'recommendations', COUNT(*) FROM recommendations
UNION ALL SELECT 'dead_letter_events', COUNT(*) FROM dead_letter_events;
"
```

---

## Commandes de debug

```bash
# Voir les logs d'une tâche
docker exec -it spotify_q-airflow-worker-1 bash -c \
  "find /opt/airflow/logs -name '*.log' -path '*<nom_tache>*' | sort | tail -1 | xargs tail -30"

# Vérifier la queue Redis
docker exec -it spotify_q-redis-1 redis-cli -n 1 llen listening_events_queue

# Redémarrer un container
docker compose restart airflow-scheduler

# Voir les logs en temps réel
docker logs spotify_q-airflow-worker-1 -f
```
