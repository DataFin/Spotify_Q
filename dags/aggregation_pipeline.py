"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend streaming_events_pipeline)
        → compute_top_tracks()      ← top 50 du jour → daily_streams
        → compute_artist_stats()    ← streams + unique_listeners → artist_stats
        → compute_p2p_metrics()     ← taux cache_hit, latence moyenne
        → update_aggregates()       ← écriture PostgreSQL
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.
Utilise `execution_date_fn` pour pointer vers le dernier run réussi
(les deux DAGs ont des schedules différents : */5 vs 0 4 * * *).

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `data_interval_start` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    def get_most_recent_run(dt, **kwargs):
        """
        Retourne la date du dernier run réussi de streaming_events_pipeline.
        Nécessaire car streaming_events_pipeline tourne toutes les 5 min
        alors que aggregation_pipeline tourne à 4h du matin — les
        execution_date ne correspondent jamais sans cette fonction.
        """
        from airflow.models import DagRun
        from airflow.utils.state import State

        runs = DagRun.find(
            dag_id="streaming_events_pipeline",
            state=State.SUCCESS,
        )
        if not runs:
            return dt
        return max(run.execution_date for run in runs)

    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        execution_date_fn=get_most_recent_run,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # ── 1. Top 50 tracks ─────────────────────────────────────────────────────
    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """
        Calcule le top 50 des tracks pour la date d'exécution.
        Stratégie incrémentale : filtre sur DATE(timestamp) = execution_date.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        exec_date = context["data_interval_end"].date()
        logging.info(f"[top_tracks] Calcul pour la date : {exec_date}")

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                track_id::text,
                COUNT(*)                        AS total_streams,
                COUNT(DISTINCT user_id)         AS unique_listeners,
                SUM(duration_ms)                AS total_duration_ms,
                ARRAY_AGG(DISTINCT geo_country) AS countries
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
              AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50
            """,
            {"date": exec_date},
        )

        rows = cursor.fetchall()
        cursor.close()

        result = [
            {
                "track_id":          row[0],
                "total_streams":     row[1],
                "unique_listeners":  row[2],
                "total_duration_ms": row[3],
                "countries":         row[4] or [],
                "date":              str(exec_date),
            }
            for row in rows
        ]

        logging.info(f"[top_tracks] {len(result)} tracks calculés")
        return result

    # ── 2. Stats artistes ────────────────────────────────────────────────────
    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """
        Calcule les statistiques par artiste pour la date d'exécution.
        Jointure listening_events × tracks pour récupérer artist_id.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        exec_date = context["data_interval_end"].date()
        logging.info(f"[artist_stats] Calcul pour la date : {exec_date}")

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                t.artist_id::text,
                COUNT(le.id)               AS total_streams,
                COUNT(DISTINCT le.user_id) AS unique_listeners,
                (
                    SELECT le2.track_id::text
                    FROM listening_events le2
                    JOIN tracks t2 ON le2.track_id = t2.id
                    WHERE t2.artist_id = t.artist_id
                      AND DATE(le2.timestamp) = %(date)s
                    GROUP BY le2.track_id
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                ) AS top_track_id
            FROM listening_events le
            JOIN tracks t ON le.track_id = t.id
            WHERE DATE(le.timestamp) = %(date)s
            GROUP BY t.artist_id
            ORDER BY total_streams DESC
            """,
            {"date": exec_date},
        )

        rows = cursor.fetchall()
        cursor.close()

        result = [
            {
                "artist_id":        row[0],
                "total_streams":    row[1],
                "unique_listeners": row[2],
                "top_track_id":     row[3],
                "date":             str(exec_date),
            }
            for row in rows
        ]

        logging.info(f"[artist_stats] {len(result)} artistes calculés")
        return result

    # ── 3. Métriques P2P ─────────────────────────────────────────────────────
    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """
        Calcule les métriques du réseau P2P pour la date d'exécution.
        - Taux de cache_hit
        - Nombre total d'events
        - Distribution par device_type et geo_country
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        exec_date = context["data_interval_end"].date()
        logging.info(f"[p2p_metrics] Calcul pour la date : {exec_date}")

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        # Taux cache_hit et total
        cursor.execute(
            """
            SELECT
                COUNT(*)                                                  AS total_events,
                SUM(CASE WHEN event_source = 'cache' THEN 1 ELSE 0 END)  AS cache_hits,
                SUM(CASE WHEN event_source = 'p2p'   THEN 1 ELSE 0 END)  AS p2p_events
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
            """,
            {"date": exec_date},
        )
        row          = cursor.fetchone()
        total_events = row[0] or 0
        cache_hits   = row[1] or 0
        p2p_events   = row[2] or 0
        cache_hit_rate = round(cache_hits / total_events, 4) if total_events > 0 else 0.0

        # Distribution par device_type
        cursor.execute(
            """
            SELECT device_type, COUNT(*) AS cnt
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
            GROUP BY device_type
            ORDER BY cnt DESC
            """,
            {"date": exec_date},
        )
        device_distribution = {row[0]: row[1] for row in cursor.fetchall()}

        # Distribution par geo_country (top 10)
        cursor.execute(
            """
            SELECT geo_country, COUNT(*) AS cnt
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
            GROUP BY geo_country
            ORDER BY cnt DESC
            LIMIT 10
            """,
            {"date": exec_date},
        )
        country_distribution = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.close()

        metrics = {
            "date":                 str(exec_date),
            "total_events":         total_events,
            "cache_hits":           cache_hits,
            "p2p_events":           p2p_events,
            "cache_hit_rate":       cache_hit_rate,
            "device_distribution":  device_distribution,
            "country_distribution": country_distribution,
        }

        logging.info(
            f"[p2p_metrics] total={total_events} | "
            f"cache_hit_rate={cache_hit_rate:.1%}"
        )
        return metrics

    # ── 4. Upsert PostgreSQL ─────────────────────────────────────────────────
    @task(task_id="update_aggregates")
    def update_aggregates(
        top_tracks: list,
        artist_stats: list,
        p2p_metrics: dict,
        **context,
    ):
        """
        Écrit les agrégats dans PostgreSQL de façon idempotente.
        ON CONFLICT DO UPDATE SET pour daily_streams et artist_stats.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not top_tracks and not artist_stats:
            logging.info("[aggregates] Aucune donnée à insérer.")
            return

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        # UPSERT daily_streams
        ds_inserted = 0
        for track in top_tracks:
            cursor.execute(
                """
                INSERT INTO daily_streams (
                    track_id, date, total_streams,
                    unique_listeners, total_duration_ms, countries, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams     = EXCLUDED.total_streams,
                    unique_listeners  = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    countries         = EXCLUDED.countries,
                    updated_at        = NOW()
                """,
                (
                    track["track_id"],
                    track["date"],
                    track["total_streams"],
                    track["unique_listeners"],
                    track["total_duration_ms"],
                    track["countries"],
                ),
            )
            ds_inserted += 1

        # UPSERT artist_stats
        as_inserted = 0
        for artist in artist_stats:
            cursor.execute(
                """
                INSERT INTO artist_stats (
                    artist_id, date, total_streams,
                    unique_listeners, top_track_id, updated_at
                ) VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (artist_id, date) DO UPDATE SET
                    total_streams    = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    top_track_id     = EXCLUDED.top_track_id,
                    updated_at       = NOW()
                """,
                (
                    artist["artist_id"],
                    artist["date"],
                    artist["total_streams"],
                    artist["unique_listeners"],
                    artist["top_track_id"],
                ),
            )
            as_inserted += 1

        conn.commit()
        cursor.close()

        # Log top track
        if top_tracks:
            top = top_tracks[0]
            logging.info(
                f"[aggregates] Top track : {top['track_id']} "
                f"avec {top['total_streams']} streams"
            )

        logging.info(
            f"[aggregates] daily_streams={ds_inserted} | "
            f"artist_stats={as_inserted} | "
            f"cache_hit_rate={p2p_metrics.get('cache_hit_rate', 0):.1%}"
        )

    # ── Orchestration ─────────────────────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
