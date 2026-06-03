# RUNBOOK.md — Spotify Data Platform

## Incidents les plus probables et procédures de résolution

---

## Incident #1 — `listening_events` reste vide après plusieurs DAGRuns

### Symptômes
```sql
SELECT COUNT(*) FROM listening_events;
-- count = 0
```

### Causes possibles et diagnostic

**Cause 1 — Le simulateur ne tourne pas**
```bash
docker exec -it spotify_q-redis-1 redis-cli -n 1 llen listening_events_queue
# Si 0 → simulateur arrêté
```
**Solution :**
```bash
python -m src.p2p_simulator.simulator --peers 10 --rate 3
```

**Cause 2 — Le simulateur utilise publish au lieu de lpush**
```bash
grep "publish\|lpush" src/p2p_simulator/simulator.py
# Si "publish" → bug
```
**Solution :**
```bash
# Corriger la ligne dans simulator.py
# self.redis.publish(channel, payload)
# →
# self.redis.lpush(channel + "_queue", payload)
```

**Cause 3 — Violation de contrainte PostgreSQL**
```bash
docker logs spotify_q-airflow-worker-1 --tail 50 2>&1 | grep "WARNING\|ERROR"
```
**Solution :** Vérifier les logs et corriger la contrainte violée (FK, type, etc.)

**Cause 4 — Catalogue vide (unknown_track)**
```bash
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT COUNT(*) FROM tracks;"
# Si 0 → lancer catalog_ingestion_pipeline
```

### Procédure de résolution complète
```bash
# 1. Vérifier la queue Redis
docker exec -it spotify_q-redis-1 redis-cli -n 1 llen listening_events_queue

# 2. Vérifier le catalogue
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "SELECT COUNT(*) FROM tracks;"

# 3. Vérifier les logs
docker logs spotify_q-airflow-worker-1 --tail 100 2>&1 | grep "WARNING\|ERROR\|inserted"

# 4. Trigger manuellement
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger streaming_events_pipeline
```

---

## Incident #2 — DAG bloqué en état `running` depuis plus de 30 minutes

### Symptômes
- Le DAGRun reste en `running` indéfiniment
- Les tâches sont en `queued` sans démarrer

### Causes possibles et diagnostic

**Cause 1 — Worker Airflow surchargé ou planté**
```bash
docker compose ps
# Vérifier que airflow-worker est Up
docker logs spotify_q-airflow-worker-1 --tail 20
```

**Cause 2 — ExternalTaskSensor en attente infinie**
```bash
# Vérifier si streaming_events_pipeline a un run SUCCESS récent
docker exec -it spotify_q-airflow-scheduler-1 airflow dags list-runs \
  -d streaming_events_pipeline
```

**Cause 3 — Trop de connexions PostgreSQL**
```bash
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT count(*), state FROM pg_stat_activity GROUP BY state;"
```
**Solution :**
```bash
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state='idle';"
```

### Procédure de résolution complète
```bash
# 1. Vider la tâche bloquée depuis l'UI Airflow
# http://localhost:8080 → DAG → tâche → Clear

# 2. Ou via CLI
docker exec -it spotify_q-airflow-scheduler-1 airflow tasks clear \
  <dag_id> -t <task_id> --yes

# 3. Redémarrer le worker si nécessaire
docker compose restart airflow-worker

# 4. Re-trigger le DAG
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger <dag_id>
```

---

## Incident #3 — `daily_streams` vide après le run de `aggregation_pipeline`

### Symptômes
```sql
SELECT COUNT(*) FROM daily_streams;
-- count = 0
```

### Causes possibles et diagnostic

**Cause 1 — Décalage de date (data_interval_start vs data_interval_end)**
```bash
# Vérifier quelle date est dans listening_events
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT DATE(timestamp), COUNT(*) FROM listening_events GROUP BY 1 ORDER BY 1 DESC;"

# Vérifier la date utilisée par le DAG
docker exec -it spotify_q-airflow-worker-1 bash -c \
  "find /opt/airflow/logs -name '*.log' -path '*compute_top*' | sort | tail -1 | xargs grep 'Calcul pour'"
```
**Solution :** S'assurer que le DAG utilise `data_interval_end.date()` et non `data_interval_start.date()`

**Cause 2 — Aucun event avec `completed=TRUE`**
```bash
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT completed, COUNT(*) FROM listening_events GROUP BY completed;"
```
**Solution :** Le filtre `completed = TRUE` dans `compute_top_tracks` est strict — vérifier que le simulateur génère bien des events avec `completed=True`

**Cause 3 — ExternalTaskSensor ne trouve pas le run de streaming_events**
```bash
# Vérifier que streaming_events_pipeline a un run SUCCESS
docker exec -it spotify_q-airflow-scheduler-1 airflow dags list-runs \
  -d streaming_events_pipeline | grep success
```

### Procédure de résolution complète
```bash
# 1. Vérifier les données source
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT DATE(timestamp), COUNT(*) FROM listening_events WHERE completed=TRUE GROUP BY 1;"

# 2. Trigger manuellement
docker exec -it spotify_q-airflow-scheduler-1 airflow dags trigger aggregation_pipeline

# 3. Vérifier le résultat
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c \
  "SELECT * FROM daily_streams ORDER BY total_streams DESC LIMIT 5;"
```

---

## Commandes de monitoring utiles

```bash
# État de tous les DAGs
docker exec -it spotify_q-airflow-scheduler-1 airflow dags list

# Derniers runs de tous les DAGs
docker exec -it spotify_q-airflow-scheduler-1 airflow dags list-runs --limit 10

# État des tables PostgreSQL
docker exec -it spotify_q-postgres-1 psql -U spotify spotify -c "
SELECT 'tracks' as table_name, COUNT(*) FROM tracks
UNION ALL SELECT 'listening_events', COUNT(*) FROM listening_events
UNION ALL SELECT 'daily_streams', COUNT(*) FROM daily_streams
UNION ALL SELECT 'recommendations', COUNT(*) FROM recommendations
UNION ALL SELECT 'dead_letter_events', COUNT(*) FROM dead_letter_events;"

# État Redis
docker exec -it spotify_q-redis-1 redis-cli -n 1 llen listening_events_queue
docker exec -it spotify_q-redis-1 redis-cli -n 1 keys "reco:*" | wc -l

# Fichiers Parquet MinIO
docker exec -it spotify_q-minio-1 mc ls local/spotify-parquet --recursive | wc -l
```
