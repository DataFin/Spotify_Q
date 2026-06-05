# Validation Issues #6, #7, #8

## Prérequis

```bash
# 1. Lancer la stack Docker
docker compose up -d
docker compose ps  # tous les containers doivent être Up ou healthy

# 2. Lancer le simulateur P2P (laisser tourner dans un terminal dédié)
python -m src.p2p_simulator.simulator --peers 10 --rate 3
```

---

## Issue #6 — streaming_events_pipeline

### Récupérer la branche
```bash
git fetch origin
git checkout feat/issue-6-streaming
```

### Trigger le DAG
```bash
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger streaming_events_pipeline
```

### Attendre 1 minute puis valider les 3 critères

**Critère 1 — PostgreSQL COUNT(*) > 0**
```bash
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM listening_events;"
```
Résultat attendu : `count > 0`

**Critère 2 — Fichiers Parquet dans MinIO**
```bash
docker exec -it spotify_q-minio-1 mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec -it spotify_q-minio-1 mc ls local/spotify-parquet --recursive
```
Résultat attendu : fichiers `listening_events/date=.../hour=.../part-*.parquet`

**Critère 3 — DLQ active**
```bash
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT error_type, COUNT(*) FROM dead_letter_events GROUP BY error_type ORDER BY count DESC;"
```
Résultat attendu : au moins une ligne dans la table

---

## Issue #7 — aggregation_pipeline

### Prérequis
L'Issue #6 doit avoir au moins un DAGRun vert.

### Récupérer la branche
```bash
git fetch origin
git checkout feat/issue-7-aggregation
```

### Trigger le DAG
```bash
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger aggregation_pipeline
```

### Attendre 2 minutes puis valider

**Critère — daily_streams peuplée**
```bash
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT * FROM daily_streams ORDER BY total_streams DESC LIMIT 10;"
```
Résultat attendu : 10 lignes avec track_id, total_streams, unique_listeners, countries

**Vérification complémentaire — artist_stats**
```bash
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM artist_stats;"
```
Résultat attendu : `count > 0`

**Vérification complémentaire — métriques P2P dans les logs**
```bash
docker logs spotify_q-airflow-worker-1 --tail 20 2>&1 | grep "cache_hit_rate"
```

---

## Issue #8 — recommendation_pipeline

### Prérequis
L'Issue #7 doit avoir au moins un DAGRun vert.

### Récupérer la branche
```bash
git fetch origin
git checkout feat/issue-8-recommendation
```

### Trigger le DAG
```bash
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger recommendation_pipeline
```

### Attendre 2 minutes puis valider

**Critère — Clés Redis présentes**
```bash
docker exec -it spotify_q-redis-1 redis-cli -n 1 keys "reco:*"
```
Résultat attendu : 500+ clés `reco:<user_id>`

**Critère — Contenu d'une clé Redis**
```bash
# Prendre une clé depuis la liste ci-dessus et vérifier
docker exec -it spotify_q-redis-1 redis-cli -n 1 get "reco:<user_id>"
```
Résultat attendu : liste de 10 track_ids JSON

**Critère — PostgreSQL recommendations**
```bash
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM recommendations;"
```
Résultat attendu : `count >= 5000` (500 users × 10 tracks)

---

## Validation globale — tous les DAGs en vert

```bash
# Vérifier l'état de tous les DAGs
docker exec -it spotify_q-airflow-scheduler-1 airflow dags list
```

Aller sur **http://localhost:8080** et vérifier que les DAGs suivants sont verts :
- `streaming_events_pipeline` ✅
- `aggregation_pipeline` ✅
- `recommendation_pipeline` ✅

---

## Résumé des résultats attendus

| Table / Stockage | Résultat attendu |
|---|---|
| `listening_events` | COUNT(*) > 1000 |
| `daily_streams` | 50 tracks avec total_streams |
| `artist_stats` | COUNT(*) > 0 |
| `recommendations` | COUNT(*) >= 5000 |
| Redis `reco:*` | 500+ clés avec TTL 24h |
| MinIO `spotify-parquet` | Fichiers Parquet partitionnés |
| `dead_letter_events` | Events avec différents statuts |
