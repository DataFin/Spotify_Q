"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'injection
```sql
INSERT INTO dead_letter_events (payload, error_type)
VALUES ('{}', 'test');
```
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """
        Récupère les événements en attente de retraitement.
        """
        hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, payload, error_type, retry_count, original_topic
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %(max_retries)s
            ORDER BY created_at ASC
            LIMIT %(batch_size)s
            """,
            {"max_retries": MAX_RETRIES, "batch_size": BATCH_SIZE},
        )

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        events = [
            {
                "id":             str(row[0]),
                "payload":        row[1],
                "error_type":     row[2],
                "retry_count":    row[3],
                "original_topic": row[4],
            }
            for row in rows
        ]

        logging.info(f"[fetch] {len(events)} événements pending trouvés")
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger et réinjecter chaque événement défectueux.
        """
        reprocessed = []
        failed      = []

        for event in pending_events:
            try:
                payload = event["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)

                user_id  = payload.get("user_id")
                track_id = payload.get("track_id")

                if not user_id:
                    failed.append({"id": event["id"], "reason": "user_id manquant"})
                    continue

                if not track_id:
                    failed.append({"id": event["id"], "reason": "track_id manquant"})
                    continue

                # timestamp invalide → fallback sur NOW()
                if not payload.get("timestamp"):
                    payload["timestamp"] = datetime.utcnow().isoformat()

                reprocessed.append({
                    "id":      event["id"],
                    "payload": payload,
                })

            except Exception as e:
                failed.append({"id": event["id"], "reason": str(e)})

        logging.info(
            f"[reprocess] {len(reprocessed)} corrigés | {len(failed)} échoués"
        )
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        Met à jour le statut des événements dans dead_letter_events.
        Utilise des SAVEPOINTs pour éviter la cascade d'erreurs de transaction.
        """
        hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

        reprocessed = results.get("reprocessed", [])
        failed      = results.get("failed", [])

        reprocessed_count = 0
        failed_ids = [e["id"] for e in failed]

        # Insérer les events valides dans listening_events + marquer reprocessed
        for event in reprocessed:
            p = event["payload"]
            try:
                cursor.execute("SAVEPOINT sp")

                # Parser le timestamp
                ts_raw = p.get("timestamp")
                if isinstance(ts_raw, str):
                    ts_raw = ts_raw.replace("Z", "+00:00")
                    try:
                        from datetime import timezone
                        ts = datetime.fromisoformat(ts_raw).replace(tzinfo=None)
                    except ValueError:
                        ts = datetime.utcnow()
                else:
                    ts = ts_raw or datetime.utcnow()

                cursor.execute(
                    """
                    INSERT INTO listening_events (
                        user_id, track_id, timestamp, duration_ms,
                        device_type, geo_country, completed, event_source
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        p.get("user_id"),
                        p.get("track_id"),
                        ts,
                        p.get("duration_ms"),
                        p.get("device_type"),
                        p.get("geo_country"),
                        p.get("completed", False),
                        p.get("event_source", "dlq"),
                    ),
                )

                cursor.execute(
                    "UPDATE dead_letter_events SET status = 'reprocessed', resolved_at = NOW() WHERE id = %s",
                    (event["id"],),
                )

                cursor.execute("RELEASE SAVEPOINT sp")
                reprocessed_count += 1

            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT sp")
                logging.warning(f"[update] Réinsertion échouée {event['id']} : {e}")
                failed_ids.append(event["id"])

        # Mettre à jour les events échoués
        for event_id in failed_ids:
            try:
                cursor.execute("SAVEPOINT sp_failed")
                cursor.execute(
                    """
                    UPDATE dead_letter_events
                    SET retry_count   = retry_count + 1,
                        last_retry_at = NOW(),
                        status = CASE
                            WHEN retry_count + 1 >= %(max_retries)s THEN 'abandoned'
                            ELSE 'pending'
                        END
                    WHERE id = %(id)s
                    """,
                    {"max_retries": MAX_RETRIES, "id": event_id},
                )
                cursor.execute("RELEASE SAVEPOINT sp_failed")
            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT sp_failed")
                logging.warning(f"[update] Mise à jour statut échouée {event_id} : {e}")

        conn.commit()
        cursor.close()
        conn.close()

        logging.info(
            f"[update] reprocessed={reprocessed_count} | failed/abandoned={len(failed_ids)}"
        )
        return {
            "reprocessed_count": reprocessed_count,
            "failed_count":      len(failed_ids),
        }

    # ── Orchestration ─────────────────────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
