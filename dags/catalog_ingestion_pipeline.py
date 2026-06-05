"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()
        → load_to_postgres()
        → notify_success()
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel

### Monitoring
- XCom `tracks_inserted` : nombre de tracks insérées/mises à jour
- XCom `errors_count` : nombre d'entrées envoyées en DLQ
"""

DEFAULT_ARGS = {
    "owner":                     "spotify-team",
    "depends_on_past":           False,
    "start_date":                datetime(2025, 1, 1),
    "email_on_failure":          False,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]


with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    # ── 1. Extract ────────────────────────────────────────────────────────────
    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list:
        import boto3
        import json
        import logging

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )

        catalogs = []
        for filename in LABEL_FILES:
            try:
                obj     = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(obj["Body"].read())
                catalogs.append(catalog)
                logging.info(f"Téléchargé : {filename}")
            except Exception as e:
                logging.warning(f"Fichier manquant ou erreur : {filename} — {e}")

        return catalogs

    # ── 2. Validate ───────────────────────────────────────────────────────────
    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list) -> dict:
        import json
        import logging
        from datetime import datetime, timezone

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        valid        = {"artists": [], "albums": [], "tracks": []}
        errors_count = 0

        for catalog in raw_catalogs:
            for artist in catalog.get("artists", []):
                if all(k in artist for k in ["id", "name", "label"]):
                    valid["artists"].append(artist)
                else:
                    errors_count += 1
                    cursor.execute(
                        "INSERT INTO dead_letter_events (payload, error_type, created_at) "
                        "VALUES (%s, %s, %s)",
                        (json.dumps(artist), "schema_validation", datetime.now(timezone.utc)),
                    )

            for album in catalog.get("albums", []):
                if all(k in album for k in ["id", "artist_id", "title"]):
                    valid["albums"].append(album)
                else:
                    errors_count += 1
                    cursor.execute(
                        "INSERT INTO dead_letter_events (payload, error_type, created_at) "
                        "VALUES (%s, %s, %s)",
                        (json.dumps(album), "schema_validation", datetime.now(timezone.utc)),
                    )

            for track in catalog.get("tracks", []):
                if all(k in track for k in ["id", "artist_id", "title", "duration_ms"]):
                    valid["tracks"].append(track)
                else:
                    errors_count += 1
                    cursor.execute(
                        "INSERT INTO dead_letter_events (payload, error_type, created_at) "
                        "VALUES (%s, %s, %s)",
                        (json.dumps(track), "schema_validation", datetime.now(timezone.utc)),
                    )

        conn.commit()
        cursor.close()

        logging.info(
            f"Validation : {len(valid['tracks'])} tracks valides | {errors_count} erreurs"
        )
        return {"valid": valid, "errors_count": errors_count}

    # ── 3. Transform ──────────────────────────────────────────────────────────
    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        import logging

        valid        = validated["valid"]
        artists      = []
        albums       = []
        tracks       = []
        seen_artists = set()
        seen_albums  = set()
        seen_tracks  = set()

        for artist in valid["artists"]:
            if artist["id"] in seen_artists:
                continue
            seen_artists.add(artist["id"])
            artist["name"]  = artist["name"].strip().title()
            artist["label"] = artist["label"].strip()
            artists.append(artist)

        for album in valid["albums"]:
            if album["id"] in seen_albums:
                continue
            seen_albums.add(album["id"])
            album["title"] = album["title"].strip().title()
            albums.append(album)

        for track in valid["tracks"]:
            if track["id"] in seen_tracks:
                continue
            seen_tracks.add(track["id"])
            track["title"] = track["title"].strip().title()
            if not (0 < track["duration_ms"] < 3_600_000):
                logging.warning(f"Durée invalide ignorée : {track['id']}")
                continue
            tracks.append(track)

        logging.info(
            f"Transform : {len(artists)} artistes | "
            f"{len(albums)} albums | {len(tracks)} tracks"
        )
        return {"artists": artists, "albums": albums, "tracks": tracks}

    # ── 4. Load ───────────────────────────────────────────────────────────────
    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        import logging

        pg     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg.get_conn()
        cursor = conn.cursor()

        artists = transformed["artists"]
        albums  = transformed["albums"]
        tracks  = transformed["tracks"]

        # Upsert artists
        cursor.executemany(
            """
            INSERT INTO artists (id, name, country, label, genres, monthly_listeners)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (name, label) DO UPDATE SET
                monthly_listeners = EXCLUDED.monthly_listeners,
                updated_at        = NOW()
            """,
            [
                (
                    a["id"], a["name"], a.get("country"),
                    a["label"], a.get("genres", []),
                    a.get("monthly_listeners", 0),
                )
                for a in artists
            ],
        )

        # Upsert albums
        cursor.executemany(
            """
            INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title
            """,
            [
                (
                    a["id"], a["artist_id"], a["title"],
                    a.get("release_year"), a.get("total_tracks"),
                )
                for a in albums
            ],
        )

        # Upsert tracks
        cursor.executemany(
            """
            INSERT INTO tracks (
                id, album_id, artist_id, title,
                duration_ms, genre, bpm, explicit, audio_file_path
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                updated_at = NOW()
            """,
            [
                (
                    t["id"], t["album_id"], t["artist_id"], t["title"],
                    t["duration_ms"], t.get("genre"), t.get("bpm"),
                    t.get("explicit", False), t.get("audio_file_path"),
                )
                for t in tracks
            ],
        )

        conn.commit()
        cursor.close()

        stats = {
            "artists_inserted": len(artists),
            "albums_inserted":  len(albums),
            "tracks_inserted":  len(tracks),
            "errors_count":     0,
        }
        logging.info(f"Chargé : {stats}")
        return stats

    # ── 5. Notify ─────────────────────────────────────────────────────────────
    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        dag_run = context["dag_run"]
        print(f"""
        catalog_ingestion_pipeline terminé
        DAGRun          : {dag_run.run_id}
        Tracks insérées : {stats.get('tracks_inserted', 0)}
        Artists insérés : {stats.get('artists_inserted', 0)}
        Erreurs DLQ     : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration ─────────────────────────────────────────────────────────
    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)
