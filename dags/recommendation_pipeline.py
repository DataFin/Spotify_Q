"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## recommendation_pipeline

### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400   # 24 heures
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    def get_most_recent_aggregation(dt, **kwargs):
        """
        Retourne la date du dernier run réussi de aggregation_pipeline.
        """
        from airflow.models import DagRun
        from airflow.utils.state import State

        runs = DagRun.find(
            dag_id="aggregation_pipeline",
            state=State.SUCCESS,
        )
        if not runs:
            return dt
        return max(run.execution_date for run in runs)

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        execution_date_fn=get_most_recent_aggregation,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # ── 1. Construire la matrice user × track ────────────────────────────────
    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Construit la matrice user × track des écoutes des 7 derniers jours.
        Ne garde que les utilisateurs avec >= 3 écoutes distinctes.
        Retourne {user_id: {track_id: play_count}}.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                user_id::text,
                track_id::text,
                COUNT(*) AS play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
              AND completed = TRUE
            GROUP BY user_id, track_id
            """
        )
        rows = cursor.fetchall()
        cursor.close()

        # Construire le dict {user_id: {track_id: play_count}}
        matrix = {}
        for user_id, track_id, play_count in rows:
            if user_id not in matrix:
                matrix[user_id] = {}
            matrix[user_id][track_id] = play_count

        # Garder uniquement les users avec >= 3 tracks distincts
        matrix = {
            user_id: tracks
            for user_id, tracks in matrix.items()
            if len(tracks) >= 3
        }

        logging.info(
            f"[matrix] {len(matrix)} utilisateurs actifs | "
            f"{len(rows)} écoutes totales"
        )
        return {"matrix": matrix, "users": list(matrix.keys())}

    # ── 2. Calculer les recommandations ──────────────────────────────────────
    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Calcule les recommandations par similarité cosinus.
        Pour chaque user : recommande les tracks aimés par ses voisins
        mais qu'il n'a pas encore écoutés.
        """
        import logging
        import numpy as np

        matrix = matrix_data.get("matrix", {})
        users  = matrix_data.get("users", [])

        if not users:
            logging.info("[reco] Aucun utilisateur actif.")
            return {}

        # Construire la liste de tous les tracks
        all_tracks = sorted({
            track_id
            for tracks in matrix.values()
            for track_id in tracks
        })
        track_index = {t: i for i, t in enumerate(all_tracks)}
        user_index  = {u: i for i, u in enumerate(users)}

        n_users  = len(users)
        n_tracks = len(all_tracks)

        # Construire la matrice numpy (users × tracks)
        M = np.zeros((n_users, n_tracks), dtype=np.float32)
        for user_id, tracks in matrix.items():
            ui = user_index[user_id]
            for track_id, count in tracks.items():
                ti = track_index[track_id]
                M[ui, ti] = count

        # Normaliser chaque vecteur user (similarité cosinus)
        norms = np.linalg.norm(M, axis=1, keepdims=True)
        norms[norms == 0] = 1
        M_norm = M / norms

        # Matrice de similarité cosinus (n_users × n_users)
        sim_matrix = M_norm @ M_norm.T

        recommendations = {}
        for user_id in users:
            ui = user_index[user_id]

            # Tracks déjà écoutés par cet user
            listened = set(matrix[user_id].keys())

            # Scores pondérés par similarité avec les voisins
            scores = np.zeros(n_tracks, dtype=np.float32)
            sim_scores = sim_matrix[ui].copy()
            sim_scores[ui] = 0  # exclure l'utilisateur lui-même

            # Top 20 voisins les plus similaires
            top_neighbors = np.argsort(sim_scores)[::-1][:20]
            for neighbor_idx in top_neighbors:
                neighbor_id = users[neighbor_idx]
                sim = sim_scores[neighbor_idx]
                if sim <= 0:
                    continue
                for track_id, count in matrix[neighbor_id].items():
                    if track_id not in listened:
                        ti = track_index[track_id]
                        scores[ti] += sim * count

            # Top N tracks recommandés
            top_tracks_idx = np.argsort(scores)[::-1][:TOP_N_RECO]
            reco_tracks = [
                (all_tracks[ti], float(scores[ti]))
                for ti in top_tracks_idx
                if scores[ti] > 0
            ]

            if reco_tracks:
                recommendations[user_id] = reco_tracks

        logging.info(
            f"[reco] {len(recommendations)} users avec recommandations "
            f"sur {n_users} utilisateurs actifs"
        )
        return recommendations

    # ── 3. Stocker les recommandations ───────────────────────────────────────
    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis (TTL 24h) et PostgreSQL.
        """
        import json
        import logging
        from datetime import datetime, timezone

        import redis
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not recommendations:
            logging.info("[store] Aucune recommandation à stocker.")
            return {"users_with_recos": 0, "total_recommendations": 0}

        # ── Redis ──────────────────────────────────────────────────────────
        r = redis.from_url(REDIS_URL, decode_responses=True)
        redis_stored = 0
        for user_id, reco_tracks in recommendations.items():
            track_ids = [t[0] for t in reco_tracks]
            r.setex(
                f"reco:{user_id}",
                RECO_TTL_SECONDS,
                json.dumps(track_ids),
            )
            redis_stored += 1

        # ── PostgreSQL ─────────────────────────────────────────────────────
        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        pg_stored = 0
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for user_id, reco_tracks in recommendations.items():
            for track_id, score in reco_tracks:
                try:
                    cursor.execute("SAVEPOINT sp")
                    cursor.execute(
                        """
                        INSERT INTO recommendations (
                            user_id, track_id, score, generated_at
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id, track_id) DO UPDATE SET
                            score        = EXCLUDED.score,
                            generated_at = EXCLUDED.generated_at
                        """,
                        (user_id, track_id, score, now),
                    )
                    cursor.execute("RELEASE SAVEPOINT sp")
                    pg_stored += 1
                except Exception as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT sp")
                    logging.warning(f"[store] Ignoré : {e} | user={user_id} track={track_id}")

        conn.commit()
        cursor.close()

        logging.info(
            f"[store] Redis={redis_stored} users | "
            f"PostgreSQL={pg_stored} recommandations"
        )
        return {
            "users_with_recos":    redis_stored,
            "total_recommendations": pg_stored,
        }

    # ── Orchestration ─────────────────────────────────────────────────────────
    matrix          = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)
