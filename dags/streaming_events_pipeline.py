from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis queue `listening_events_queue`
- Redis queue `p2p_network_queue`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés par heure sur MinIO (`spotify-parquet`)
- Table `dead_letter_events` (events invalides / enrichissement impossible)

### Flux conditionnel
```
consume_from_redis
        │
        ├──► validate_listening  ──► enrich_events ──► store_to_parquet
        │                                          └──► upsert_to_postgres
        │
        └──► validate_p2p ──► store_p2p_to_parquet
```

### Idempotence
Chaque event est identifié par `event_id` (UUID).
`ON CONFLICT (id) DO NOTHING` évite les doublons en PostgreSQL.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID  = "spotify_postgres"
REDIS_URL         = "redis://redis:6379/1"
MINIO_ENDPOINT    = "http://minio:9000"
MINIO_BUCKET      = "spotify-parquet"
BATCH_SIZE_L      = 1000   # max listening events par run
BATCH_SIZE_P2P    = 500    # max p2p events par run


# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    # ── 1. Consommation Redis ─────────────────────────────────────────────────
    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """
        Dépile jusqu'à BATCH_SIZE messages de chaque queue Redis.
        Retourne deux listes brutes : listening et p2p_network.
        """
        import redis
        import json
        import logging

        r = redis.from_url(REDIS_URL, decode_responses=True)
        listening, p2p = [], []

        for _ in range(BATCH_SIZE_L):
            msg = r.rpop("listening_events_queue")
            if not msg:
                break
            try:
                listening.append(json.loads(msg))
            except Exception as e:
                logging.warning(f"[consume] JSON invalide (listening) : {e}")

        for _ in range(BATCH_SIZE_P2P):
            msg = r.rpop("p2p_network_queue")
            if not msg:
                break
            try:
                p2p.append(json.loads(msg))
            except Exception as e:
                logging.warning(f"[consume] JSON invalide (p2p) : {e}")

        logging.info(
            f"[consume] Consommé : {len(listening)} listening | {len(p2p)} p2p"
        )
        return {"listening": listening, "p2p_network": p2p}

    # ── 2a. Validation – Listening Events ────────────────────────────────────
    @task(task_id="validate_listening")
    def validate_listening(raw_events: dict, **context) -> list:
        """
        Valide les listening_events.
        Champs obligatoires : event_id, user_id, track_id, timestamp, duration_ms.
        Events invalides → dead_letter_events (DLQ).
        Retourne la liste des events valides.
        """
        import json
        import logging
        from datetime import datetime, timezone
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        REQUIRED = {"event_id", "user_id", "track_id", "timestamp", "duration_ms"}

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        valid, errors = [], 0

        for event in raw_events.get("listening", []):
            reason = None
            if not REQUIRED.issubset(event):
                reason = f"missing_fields:{REQUIRED - event.keys()}"
            elif not isinstance(event["duration_ms"], (int, float)) or event["duration_ms"] <= 0:
                reason = "invalid_duration_ms"

            if reason:
                errors += 1
                cursor.execute(
                    "INSERT INTO dead_letter_events (payload, error_type, created_at) "
                    "VALUES (%s, %s, %s)",
                    (json.dumps(event), reason, datetime.now(timezone.utc)),
                )
            else:
                valid.append(event)

        conn.commit()
        cursor.close()
        logging.info(
            f"[validate_listening] valides={len(valid)} | erreurs={errors}"
        )
        return valid

    # ── 2b. Validation – P2P Network Events ──────────────────────────────────
    @task(task_id="validate_p2p")
    def validate_p2p(raw_events: dict, **context) -> list:
        """
        Valide les p2p_network_events.
        Champs obligatoires : event_id, event_type, peer_id, timestamp.
        Events invalides → dead_letter_events (DLQ).
        Retourne la liste des events valides.
        """
        import json
        import logging
        from datetime import datetime, timezone
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        REQUIRED = {"event_id", "event_type", "peer_id", "timestamp"}

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        valid, errors = [], 0

        for event in raw_events.get("p2p_network", []):
            if not REQUIRED.issubset(event):
                missing = REQUIRED - event.keys()
                errors += 1
                cursor.execute(
                    "INSERT INTO dead_letter_events (payload, error_type, created_at) "
                    "VALUES (%s, %s, %s)",
                    (
                        json.dumps(event),
                        f"missing_fields:{missing}",
                        datetime.now(timezone.utc),
                    ),
                )
            else:
                valid.append(event)

        conn.commit()
        cursor.close()
        logging.info(
            f"[validate_p2p] valides={len(valid)} | erreurs={errors}"
        )
        return valid

    # ── 3. Enrichissement (listening uniquement) ──────────────────────────────
    @task(task_id="enrich_events")
    def enrich_events(valid_listening: list, **context) -> list:
        """
        Joint les listening_events avec le catalogue de tracks (PostgreSQL).
        track_id → title, artist_id, genre.
        Events sans correspondance → dead_letter_events (unknown_track).
        """
        import json
        import logging
        from datetime import datetime, timezone
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not valid_listening:
            logging.info("[enrich] Aucun événement à enrichir.")
            return []

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        track_ids = list({e["track_id"] for e in valid_listening})
        cursor.execute(
            "SELECT id, title, artist_id, genre FROM tracks WHERE id = ANY(%s::uuid[])",
            (track_ids,),
        )
        tracks_map = {
            str(row[0]): {"title": row[1], "artist_id": str(row[2]), "genre": row[3]}
            for row in cursor.fetchall()
        }

        enriched, missing = [], 0
        for event in valid_listening:
            track = tracks_map.get(event["track_id"])
            if track:
                event["track_title"] = track["title"]
                event["artist_id"]   = track["artist_id"]
                event["genre"]       = track["genre"]
                enriched.append(event)
            else:
                missing += 1
                cursor.execute(
                    "INSERT INTO dead_letter_events (payload, error_type, created_at) "
                    "VALUES (%s, %s, %s)",
                    (json.dumps(event), "unknown_track", datetime.now(timezone.utc)),
                )

        conn.commit()
        cursor.close()
        logging.info(
            f"[enrich] enrichis={len(enriched)} | unknown_track={missing}"
        )
        return enriched

    # ── 4a. Stockage Parquet – Listening (enrichis) ───────────────────────────
    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Écrit les listening_events enrichis en Parquet sur MinIO.
        Partitionnement : date= / hour=
        Clé : listening_events/date=YYYY-MM-DD/hour=HH/part-<run_id>.parquet
        """
        import io
        import logging

        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        if not enriched_events:
            logging.info("[store_parquet] Aucun événement à sauvegarder.")
            return "no_data"

        df = pd.DataFrame(enriched_events)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["timestamp"].dt.strftime("%H")

        date   = df["date"].iloc[0]
        hour   = df["hour"].iloc[0]
        run_id = context["run_id"]

        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buffer)
        buffer.seek(0)

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        try:
            s3.create_bucket(Bucket=MINIO_BUCKET)
        except Exception:
            pass  # bucket déjà existant

        key = f"listening_events/date={date}/hour={hour}/part-{run_id}.parquet"
        s3.upload_fileobj(buffer, MINIO_BUCKET, key)
        path = f"s3://{MINIO_BUCKET}/{key}"
        logging.info(f"[store_parquet] Sauvegardé : {path}")
        return path

    # ── 4b. Stockage Parquet – P2P ────────────────────────────────────────────
    @task(task_id="store_p2p_to_parquet")
    def store_p2p_to_parquet(valid_p2p: list, **context) -> str:
        """
        Écrit les p2p_network_events valides en Parquet sur MinIO.
        Partitionnement : date= / hour=
        Clé : p2p_events/date=YYYY-MM-DD/hour=HH/part-<run_id>.parquet
        """
        import io
        import logging

        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        if not valid_p2p:
            logging.info("[store_p2p_parquet] Aucun événement P2P à sauvegarder.")
            return "no_data"

        df = pd.DataFrame(valid_p2p)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["timestamp"].dt.strftime("%H")

        date   = df["date"].iloc[0]
        hour   = df["hour"].iloc[0]
        run_id = context["run_id"]

        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buffer)
        buffer.seek(0)

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        try:
            s3.create_bucket(Bucket=MINIO_BUCKET)
        except Exception:
            pass

        key = f"p2p_events/date={date}/hour={hour}/part-{run_id}.parquet"
        s3.upload_fileobj(buffer, MINIO_BUCKET, key)
        path = f"s3://{MINIO_BUCKET}/{key}"
        logging.info(f"[store_p2p_parquet] Sauvegardé : {path}")
        return path

    # ── 5. Upsert PostgreSQL – Listening ──────────────────────────────────────
    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """
        Insère les listening_events enrichis dans PostgreSQL.
        ON CONFLICT (id) DO NOTHING → idempotent.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not enriched_events:
            logging.info("[upsert] Aucun événement à insérer.")
            return {"inserted": 0, "skipped": 0}

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        inserted = skipped = 0
        for event in enriched_events:
            try:
                from datetime import datetime, timezone
                ts_raw = event["timestamp"]
                if isinstance(ts_raw, str):
                    ts_raw = ts_raw.replace("Z", "+00:00")
                    ts = datetime.fromisoformat(ts_raw).replace(tzinfo=None)
                else:
                    ts = ts_raw

                cursor.execute("SAVEPOINT sp")
                cursor.execute(
                    """
                    INSERT INTO listening_events (
                        id, user_id, track_id, source_peer_id, timestamp,
                        duration_ms, device_type, geo_country, completed, event_source
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        event["event_id"],
                        event["user_id"],
                        event["track_id"],
                        None,  # source_peer_id FK vers peers vide
                        ts,
                        event["duration_ms"],
                        event.get("device_type"),
                        event.get("geo_country"),
                        event.get("completed", False),
                        event.get("event_source"),
                    ),
                )
                cursor.execute("RELEASE SAVEPOINT sp")
                inserted += 1
            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT sp")
                skipped += 1
                logging.warning(
                    f"[upsert] Ignoré : {e} | event_id={event.get('event_id')}"
                )

        conn.commit()
        cursor.close()
        logging.info(f"[upsert] inserted={inserted} | skipped={skipped}")
        return {"inserted": inserted, "skipped": skipped}

    # ── Câblage du graphe de tâches ───────────────────────────────────────────
    #
    #   consume_from_redis
    #        │
    #        ├──► validate_listening ──► enrich_events ──► store_to_parquet
    #        │                                         └──► upsert_to_postgres
    #        │
    #        └──► validate_p2p ──► store_p2p_to_parquet
    #
    raw = consume_from_redis()

    # Branche listening
    valid_l  = validate_listening(raw)
    enriched = enrich_events(valid_l)
    store_to_parquet(enriched)
    upsert_to_postgres(enriched)

    # Branche p2p (indépendante)
    valid_p2p = validate_p2p(raw)
    store_p2p_to_parquet(valid_p2p)
