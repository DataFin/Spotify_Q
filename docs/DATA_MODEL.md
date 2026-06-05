# DATA_MODEL.md — Spotify Data Platform

## Tables (11 au total)

### Module 1 — Catalogue musical

| Table | Rôle |
|---|---|
| `genres` | Référentiel des genres musicaux |
| `artists` | Artistes avec label, pays, genres, monthly_listeners |
| `albums` | Albums liés aux artistes |
| `tracks` | Tracks liées aux albums et artistes |

### Module 1 — Réseau P2P et utilisateurs

| Table | Rôle |
|---|---|
| `peers` | Peers du réseau P2P (device, pays, tracks en cache) |

### Module 1 — Événements d'écoute

| Table | Rôle |
|---|---|
| `listening_events` | Événements d'écoute générés par le simulateur P2P |

### Module 1 — Agrégats batch

| Table | Rôle |
|---|---|
| `daily_streams` | Top tracks par jour (alimenté par aggregation_pipeline) |
| `artist_stats` | Stats artistes par jour |
| `recommendations` | Recommandations personnalisées par user |

### Module 1 — Dead Letter Queue

| Table | Rôle |
|---|---|
| `dead_letter_events` | Événements défectueux isolés pour retraitement |

### Module 2 — Temps réel (Spark)

| Table | Rôle |
|---|---|
| `realtime_top_tracks` | Top tracks en fenêtres de 5 min (alimenté par Spark) |
| `fraud_detections` | Détections de fraude (bot_stream, free_rider, burst_listen) |

### Module 3 — Inter-groupes

| Table | Rôle |
|---|---|
| `federated_catalog` | Catalogue fédéré entre tous les groupes |

---

## Diagramme ERD

```
┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│   genres    │       │   artists   │       │    albums   │
│─────────────│       │─────────────│       │─────────────│
│ id (PK)     │       │ id (PK)     │◄──────│ artist_id   │
│ name        │       │ name        │       │ id (PK)     │
│ created_at  │       │ country     │       │ title       │
└─────────────┘       │ label       │       │ release_year│
                      │ genres[]    │       └──────┬──────┘
                      │ monthly_... │              │
                      └──────┬──────┘              │
                             │                     │
                             └──────────┐          │
                                        ▼          ▼
                                   ┌─────────────────┐
                                   │     tracks      │
                                   │─────────────────│
                                   │ id (PK)         │
                                   │ album_id (FK)   │
                                   │ artist_id (FK)  │
                                   │ title           │
                                   │ duration_ms     │
                                   │ genre           │
                                   │ bpm             │
                                   └────────┬────────┘
                                            │
              ┌─────────────┐              │
              │    peers    │              │
              │─────────────│              │
              │ id (PK)     │◄─────┐       │
              │ peer_name   │      │       │
              │ device_type │      │       │
              │ geo_country │      │       │
              └─────────────┘      │       │
                                   │       │
                          ┌────────┴───────┴────────┐
                          │    listening_events     │
                          │─────────────────────────│
                          │ id (PK)                 │
                          │ user_id                 │
                          │ track_id (FK)           │
                          │ source_peer_id (FK)     │
                          │ timestamp  ◄── INDEX    │
                          │ duration_ms             │
                          │ device_type             │
                          │ geo_country             │
                          │ completed               │
                          │ event_source            │
                          └────────┬────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                     ▼
   ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
   │  daily_streams   │  │  artist_stats    │  │ recommendations  │
   │──────────────────│  │──────────────────│  │──────────────────│
   │ track_id (PK,FK) │  │ artist_id(PK,FK) │  │ user_id (PK)     │
   │ date (PK)        │  │ date (PK)        │  │ track_id (PK,FK) │
   │ total_streams    │  │ total_streams    │  │ score            │
   │ unique_listeners │  │ unique_listeners │  │ generated_at     │
   │ total_duration_ms│  │ top_track_id     │  └──────────────────┘
   │ countries[]      │  └──────────────────┘
   └──────────────────┘

   ┌──────────────────────────────┐
   │      dead_letter_events      │
   │──────────────────────────────│
   │ id (PK)                      │
   │ payload (JSONB)              │
   │ error_type                   │
   │ retry_count                  │
   │ status  ◄── INDEX            │
   │ created_at ◄── INDEX         │
   │ resolved_at                  │
   └──────────────────────────────┘

   ┌──────────────────────────────┐
   │     realtime_top_tracks      │  ← alimenté par Spark
   │──────────────────────────────│
   │ window_start (PK)            │
   │ track_id (PK, FK)            │
   │ stream_count                 │
   │ unique_listeners             │
   └──────────────────────────────┘
```

---

## Réponses aux questions

### Pourquoi `listening_events` est indexé sur `(timestamp)` ET `date_trunc('hour', timestamp)` ?

Les deux index répondent à des besoins différents.

L'index sur `timestamp` sert aux requêtes avec des plages de dates, par exemple
`WHERE timestamp >= '2026-06-01' AND timestamp < '2026-06-02'`. PostgreSQL peut
utiliser cet index pour scanner uniquement la période concernée sans lire toute la table.

L'index sur `date_trunc('hour', timestamp)` est un **index fonctionnel** qui sert
aux requêtes d'agrégation par heure, comme celles du DAG `aggregation_pipeline`
qui groupe les événements par heure pour générer les fichiers Parquet partitionnés
`date=/hour=`. Sans cet index, PostgreSQL devrait calculer `date_trunc` pour chaque
ligne avant de filtrer — avec l'index, le résultat est précalculé et la requête
est instantanée même sur des millions de lignes.

---

### Quelle est la différence entre `daily_streams` (batch) et `realtime_top_tracks` (Spark) ?

| Critère | `daily_streams` | `realtime_top_tracks` |
|---|---|---|
| **Source** | `aggregation_pipeline` (Airflow) | `streaming_trends_job` (Spark) |
| **Latence** | Quotidienne (calculé à 4h du matin) | Quasi temps réel (fenêtres de 5 min) |
| **Granularité** | Par jour | Par fenêtre temporelle (window_start / window_end) |
| **Usage** | Rapports, recommandations, Top 50 du lendemain | Dashboard live, alertes fraude, Top 50 instantané |
| **Volume** | 1 ligne par track par jour | Plusieurs lignes par track (une par fenêtre) |
| **Garantie** | Exactement une fois (idempotent via ON CONFLICT) | Exactly-once via checkpoints Spark sur MinIO |

En résumé : `daily_streams` est la version **consolidée et fiable** pour l'analyse
historique, `realtime_top_tracks` est la version **fraîche mais provisoire** pour
l'affichage en direct.

---

### Pourquoi `dead_letter_events.payload` est `JSONB` plutôt que `TEXT` ?

Trois raisons principales :

**1. Requêtes sur le contenu** — avec `JSONB`, on peut filtrer directement sur les
champs imbriqués : `WHERE payload->>'error_type' = 'unknown_track'` ou
`WHERE payload->>'track_id' = '...'`. Avec `TEXT`, il faudrait parser le JSON
en application avant de filtrer.

**2. Index GIN** — `JSONB` supporte les index GIN (`CREATE INDEX ON dead_letter_events
USING GIN (payload)`) qui permettent des recherches ultra-rapides sur n'importe
quel champ du JSON, même dans des structures imbriquées.

**3. Validation et stockage optimisé** — PostgreSQL valide que le contenu est du
JSON valide à l'insertion et stocke en format binaire décompressé, ce qui est plus
rapide à lire qu'un `TEXT` qu'il faudrait parser à chaque requête.

