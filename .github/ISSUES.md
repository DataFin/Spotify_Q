# Issues GitHub SPOTIFY — Les 25 livrables

Ce fichier contient le contenu de toutes les issues à créer dans GitHub.
Créer chaque issue manuellement ou via `gh issue create`.

---

## PHASE 1 — Data Pipelines Batch (Issues #1 à #10)

---

### Issue #1 — Setup Docker Compose et vérification de la stack

**Labels :** `phase-1` `infrastructure` `lundi`
**Milestone :** Phase 1 — Batch

#### Contexte
La stack SPOTIFY repose sur Docker Compose. Ce premier livrable consiste à démarrer l'environnement complet et à vérifier que chaque service répond.

#### Ce qu'on attend

- [ ] `docker compose up -d` sans erreur
- [ ] PostgreSQL accessible sur le port 5432 (tester avec `psql` ou DBeaver)
- [ ] Redis accessible sur le port 6379 (`redis-cli ping` → PONG)
- [ ] MinIO accessible sur http://localhost:9001 (UI MinIO)
- [ ] Airflow accessible sur http://localhost:8080 (admin / admin)
- [ ] La base `spotify` PostgreSQL existe avec toutes les tables
- [ ] Les buckets MinIO sont créés (`spotify-parquet`, `spotify-checkpoints`, `labels-raw`)

#### Critère de validation
Screenshot de l'UI Airflow + `docker compose ps` montrant tous les services `Up`.

#### Hints
- Si Airflow met du temps à démarrer, attendre 60s : le webserver démarre après le scheduler
- Si PostgreSQL refuse la connexion : vérifier que le port 5432 n'est pas déjà utilisé (`lsof -i :5432`)
- Les logs : `docker compose logs airflow-scheduler -f`

---

### Issue #2 — Schéma PostgreSQL et modèle de données SPOTIFY

**Labels :** `phase-1` `database` `lundi`
**Milestone :** Phase 1 — Batch

#### Contexte
Le schéma SQL est fourni dans `sql/init_spotify_db.sql`. Votre travail : comprendre le modèle de données et l'enrichir si nécessaire.

#### Ce qu'on attend

- [ ] Vérifier que toutes les tables existent et ont les bons index
- [ ] Produire un diagramme ERD (draw.io, dbdiagram.io, ou autre) et l'ajouter dans `docs/DATA_MODEL.md`
- [ ] Répondre à ces questions dans `docs/DATA_MODEL.md` :
  - Pourquoi `listening_events` est indexé sur `(timestamp)` ET sur `date_trunc('hour', timestamp)` ?
  - Quelle est la différence entre `daily_streams` (agrégat batch) et `realtime_top_tracks` (Spark) ?
  - Pourquoi `dead_letter_events.payload` est de type `JSONB` plutôt que `TEXT` ?
- [ ] Choisir ETL ou ELT pour chaque pipeline et justifier dans `docs/ARCHITECTURE.md`

#### Critère de validation
Diagramme ERD lisible + `docs/DATA_MODEL.md` complété avec les réponses.

---

### Issue #3 — Data Generator : catalogue musical avec Faker

**Labels :** `phase-1` `data` `lundi`
**Milestone :** Phase 1 — Batch

#### Contexte
Le fichier `src/data_generator/generate_catalog.py` est fourni. Vous devez le faire tourner pour générer les catalogues des 3 labels, puis les uploader dans MinIO.

#### Ce qu'on attend

- [ ] Installer les dépendances : `pip install faker boto3`
- [ ] Lancer le générateur : `python -m src.data_generator.generate_catalog --artists 15`
- [ ] 3 fichiers JSON générés dans `data/labels/`
- [ ] Uploader les 3 fichiers dans le bucket `labels-raw` de MinIO
- [ ] Vérifier que les tests `tests/unit/test_transformations.py::TestDataGenerator` passent
- [ ] Vérifier que chaque JSON contient bien : `artists`, `albums`, `tracks`

#### Critère de validation
`pytest tests/unit/test_transformations.py::TestDataGenerator -v` → 4 tests PASSED.

#### Hints
Upload vers MinIO avec boto3 :
```python
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:9000',
                  aws_access_key_id='minioadmin',
                  aws_secret_access_key='minioadmin')
s3.upload_file('data/labels/sunset_records.json', 'labels-raw', 'sunset_records.json')
```

---

### Issue #4 — DAG catalog_ingestion_pipeline

**Labels :** `phase-1` `airflow` `dag` `lundi`
**Milestone :** Phase 1 — Batch

#### Contexte
Le squelette du DAG est dans `dags/catalog_ingestion_pipeline.py`. Les tâches lèvent des `NotImplementedError`. Votre travail : les implémenter.

#### Ce qu'on attend

- [ ] `extract_from_minio()` : télécharger les 3 JSONs depuis MinIO
- [ ] `validate_schema()` : vérifier les champs obligatoires, envoyer les invalides en DLQ
- [ ] `transform_catalog()` : normaliser noms d'artistes, valider durées, aligner les genres
- [ ] `load_to_postgres()` : upsert dans `artists`, `albums`, `tracks` (idempotent)
- [ ] `notify_success()` : log avec statistiques
- [ ] Le DAG s'exécute dans l'UI Airflow sans erreur (toutes les tâches vertes)
- [ ] Relancer le DAG 2× : même résultat (idempotence vérifiée)
- [ ] Test : `pytest tests/structure/test_dag_structure.py::TestCatalogIngestionDAG -v`

#### Critère de validation
Screenshot UI Airflow : DAGRun vert + test pytest PASSED.

#### Hints
- Pour lire depuis MinIO avec boto3 dans Airflow, utiliser la connexion `spotify_minio` ou lire `MINIO_ENDPOINT` depuis les env vars
- Upsert PostgreSQL : `INSERT INTO artists (...) ON CONFLICT (name, label) DO UPDATE SET updated_at = NOW()`
- XCom : `context['ti'].xcom_push(key='tracks_inserted', value=count)`

---

### Issue #5 — Simulateur P2P : compléter et lancer

**Labels :** `phase-1` `simulator` `mardi`
**Milestone :** Phase 1 — Batch

#### Contexte
Le simulateur `src/p2p_simulator/simulator.py` génère des événements. Les méthodes `_generate_listening_event()`, `_generate_p2p_network_event()` et `_publish_to_redis()` sont à implémenter.

#### Ce qu'on attend

- [ ] Implémenter `_generate_listening_event()` avec tous les champs listés dans le TODO
- [ ] Implémenter `_generate_p2p_network_event()` avec les 5 types d'événements
- [ ] Implémenter `_publish_to_redis()` avec gestion d'exception
- [ ] Lancer le simulateur : `python -m src.p2p_simulator.simulator --peers 10 --rate 3`
- [ ] Vérifier les événements dans Redis : `redis-cli subscribe listening_events`
- [ ] Le simulateur tourne en continu sans crash pendant 5 minutes

#### Critère de validation
Output `redis-cli subscribe listening_events` montrant des events JSON en continu.

#### Hints
- `redis.publish(channel, json.dumps(event))` pour publier
- Vérifier la connexion Redis au démarrage avant la boucle principale
- Le simulateur doit gérer `SIGTERM` proprement (déjà implémenté dans `_shutdown`)

---

### Issue #6 — DAG streaming_events_pipeline

**Labels :** `phase-1` `airflow` `dag` `mardi`
**Milestone :** Phase 1 — Batch

#### Contexte
Ce DAG consomme les événements d'écoute depuis Redis (pub/sub), les valide, les enrichit avec le catalogue, et les stocke en Parquet (MinIO) + agrégats (PostgreSQL).

#### Ce qu'on attend

Créer `dags/streaming_events_pipeline.py` avec :
- [ ] Tâche `consume_from_redis` : accumuler les events sur une fenêtre temporelle (micro-batch 5 min)
- [ ] Tâche `validate_events` : valider les champs, envoyer les invalides en DLQ
- [ ] Tâche `enrich_events` : joindre avec le catalogue PostgreSQL (track_id → artiste, genre)
- [ ] Tâche `store_to_parquet` : sauvegarder les events enrichis en Parquet sur MinIO (partitionné par heure)
- [ ] Tâche `upsert_to_postgres` : insérer dans `listening_events`
- [ ] Utiliser **TaskFlow API** avec décorateurs `@task`
- [ ] Branches conditionnelles : séparer listening_events et p2p_network_events

#### Critère de validation
DAGRun vert + fichiers Parquet visibles dans MinIO + `SELECT COUNT(*) FROM listening_events` > 0.

---

### Issue #7 — DAG aggregation_pipeline + stockage MinIO

**Labels :** `phase-1` `airflow` `dag` `mardi`
**Milestone :** Phase 1 — Batch

#### Contexte
Ce DAG attend la fin du `streaming_events_pipeline` via `ExternalTaskSensor` et calcule les agrégats quotidiens.

#### Ce qu'on attend

Créer `dags/aggregation_pipeline.py` avec :
- [ ] `ExternalTaskSensor` qui attend `streaming_events_pipeline`
- [ ] Calcul des top 50 tracks du jour (par `stream_count`, table `daily_streams`)
- [ ] Calcul des stats artistes (streams, unique_listeners, table `artist_stats`)
- [ ] Calcul des métriques P2P (taux cache_hit, latence moyenne)
- [ ] Stratégie incrémentale : calculer uniquement pour le jour courant
- [ ] Test : `pytest tests/structure/test_dag_structure.py::TestAggregationDAG -v`

#### Critère de validation
`SELECT * FROM daily_streams ORDER BY total_streams DESC LIMIT 10` retourne des résultats.

---

### Issue #8 — DAG recommendation_pipeline

**Labels :** `phase-1` `airflow` `dag` `mardi`
**Milestone :** Phase 1 — Batch

#### Contexte
Pipeline de recommandation basé sur le collaborative filtering simplifié (similarité cosinus entre profils d'écoute). Les recommandations sont stockées dans Redis pour un accès rapide.

#### Ce qu'on attend

Créer `dags/recommendation_pipeline.py` avec :
- [ ] Dépendance sur `aggregation_pipeline` (ExternalTaskSensor)
- [ ] Construction de la matrice user/track depuis `listening_events`
- [ ] Calcul de similarité cosinus (scikit-learn ou implémentation custom)
- [ ] Génération de top-10 recommandations par utilisateur actif
- [ ] Stockage dans Redis : clé `reco:{user_id}` avec TTL 24h
- [ ] Stockage dans PostgreSQL table `recommendations`

#### Critère de validation
`redis-cli get reco:<un_user_id>` retourne une liste de track_ids.

---

### Issue #9 — DAG dlq_reprocessing_pipeline

**Labels :** `phase-1` `airflow` `dag` `mardi`
**Milestone :** Phase 1 — Batch

#### Contexte
Les événements défectueux sont isolés dans `dead_letter_events`. Ce DAG tente périodiquement de les retraiter.

#### Ce qu'on attend

Créer `dags/dlq_reprocessing_pipeline.py` avec :
- [ ] Sélectionner les events `status='pending'` dans `dead_letter_events`
- [ ] Tenter de retraiter chaque event (valider, corriger, réinjecter)
- [ ] Si succès : `status='reprocessed'`, `resolved_at=NOW()`
- [ ] Si échec après 3 tentatives : `status='abandoned'`
- [ ] Planification toutes les heures (`@hourly`)
- [ ] Injecter volontairement des données corrompues pour valider : `INSERT INTO dead_letter_events (payload, error_type) VALUES ('{"broken": true}', 'test')`

#### Critère de validation
`SELECT status, COUNT(*) FROM dead_letter_events GROUP BY status` montre des transitions de status.

---

### Issue #10 — Tests pytest + README + doc_md

**Labels :** `phase-1` `qualité` `documentation` `mardi`
**Milestone :** Phase 1 — Batch

#### Contexte
Clôturer la Phase 1 avec tests, documentation et revue qualité.

#### Ce qu'on attend

- [ ] `pytest tests/structure/ -v` → tous les tests PASSED (0 FAILED)
- [ ] `pytest tests/unit/ -v` → décommenter et implémenter les tests marqués `skip`
- [ ] Ajouter `doc_md` sur chaque DAG (voir modèle dans `catalog_ingestion_pipeline.py`)
- [ ] Compléter `README.md` avec votre diagramme d'architecture réel (pas le placeholder)
- [ ] `docs/RUNBOOK.md` : documenter les 3 incidents les plus probables + procédure de résolution

#### Critère de validation
`pytest tests/ -v --tb=short` → aucun FAILED.

---

## PHASE 2 — Streaming & Temps Réel (Issues #11 à #20)

---

### Issue #11 — Cluster Kafka KRaft dans docker-compose

**Labels :** `phase-2` `infrastructure` `kafka` `mercredi`
**Milestone :** Phase 2 — Streaming

#### Contexte
Décommenter le bloc Kafka dans `docker-compose.yml` et démarrer un cluster 3 brokers en mode KRaft.

#### Ce qu'on attend

- [ ] Décommenter les services `kafka-1`, `kafka-2`, `kafka-3`, `kafka-ui`, `kafka-init`
- [ ] `docker compose up -d kafka-1 kafka-2 kafka-3 kafka-ui kafka-init`
- [ ] Kafka UI accessible sur http://localhost:8090
- [ ] Les 6 topics internes sont créés (vérifier dans Kafka UI)
- [ ] Topic `listening_events` : 6 partitions, réplication factor 3
- [ ] Topic `catalog_updates` : compaction activée

#### Critère de validation
Screenshot Kafka UI montrant les 6 topics avec leurs configurations.

#### Hints
- Kafka KRaft nécessite que les 3 brokers aient le même `KAFKA_CLUSTER_ID`
- Si les brokers ne se connectent pas entre eux, vérifier le réseau Docker
- **RAM nécessaire : 16 Go minimum sur votre machine**

---

### Issue #12 — Migration simulateur P2P vers Kafka

**Labels :** `phase-2` `simulator` `kafka` `mercredi`
**Milestone :** Phase 2 — Streaming

#### Contexte
Adapter le simulateur P2P pour publier dans Kafka tout en conservant la publication Redis (les DAGs Airflow Phase 1 continuent de fonctionner).

#### Ce qu'on attend

- [ ] Installer `confluent-kafka` : `pip install confluent-kafka`
- [ ] Décommenter et implémenter `_publish_to_kafka()` dans `simulator.py`
- [ ] Le simulateur publie simultanément dans Redis ET Kafka
- [ ] Vérifier dans Kafka UI que les events arrivent bien dans `listening_events`
- [ ] Vérifier que les DAGs Airflow Phase 1 continuent de fonctionner (Redis toujours actif)
- [ ] Configurer `acks='all'` et `enable.idempotence=True` sur le producteur

#### Critère de validation
Kafka UI → topic `listening_events` → Messages : events JSON en flux continu + DAGs Phase 1 toujours verts.

---

### Issue #13 — Premier job Spark : lecture topics, affichage console

**Labels :** `phase-2` `spark` `mercredi`
**Milestone :** Phase 2 — Streaming

#### Contexte
Décommenter le bloc Spark dans `docker-compose.yml` et écrire le premier job qui lit `listening_events` et affiche les events en console.

#### Ce qu'on attend

- [ ] Décommenter `spark-master` et `spark-worker-1` dans docker-compose
- [ ] Compléter `read_kafka_stream()` dans `spark_jobs/streaming_trends_job.py`
- [ ] Écrire un job minimal avec sink `console` en mode `append`
- [ ] Lancer : `docker exec spark-master spark-submit --packages ... spark_jobs/streaming_trends_job.py`
- [ ] Observer les events s'afficher dans les logs Spark
- [ ] Expérimenter avec les trigger modes : `processingTime("10 seconds")` vs `Once`

#### Critère de validation
Events JSON visibles dans les logs Spark (`docker logs spark-master -f`).

#### Hints
- Package Kafka pour Spark : `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`
- Package PostgreSQL : `org.postgresql:postgresql:42.7.1`
- Configurer le checkpoint même pour le job de test (MinIO)

---

### Issue #14 — Job streaming_trends_job : fenêtres temporelles

**Labels :** `phase-2` `spark` `jeudi`
**Milestone :** Phase 2 — Streaming

#### Contexte
Implémenter les agrégations streaming avec fenêtres temporelles dans `spark_jobs/streaming_trends_job.py`.

#### Ce qu'on attend

- [ ] Implémenter `compute_top_tracks_tumbling()` : top 10 tracks par window de 5 min
- [ ] Écriture dans PostgreSQL table `realtime_top_tracks` via `foreachBatch`
- [ ] Implémenter `compute_genre_listeners_sliding()` : sliding 15 min / slide 5 min
- [ ] Jointure stream-static avec le catalogue PostgreSQL pour récupérer les genres
- [ ] Écriture dans Redis (`genre_listeners:live`)
- [ ] Observer les résultats se mettre à jour : `watch -n 5 "psql -c 'SELECT * FROM realtime_top_tracks ORDER BY stream_count DESC LIMIT 5'"`

#### Critère de validation
La table `realtime_top_tracks` se met à jour automatiquement toutes les 5 minutes.

---

### Issue #15 — Watermarking et gestion des late events

**Labels :** `phase-2` `spark` `jeudi`
**Milestone :** Phase 2 — Streaming

#### Contexte
Configurer le watermarking sur tous les jobs Spark et router les late events vers Airflow.

#### Ce qu'on attend

- [ ] Ajouter `.withWatermark("event_time", "10 minutes")` sur les fenêtres
- [ ] Activer le mode `late_events` du simulateur : `--mode late_events`
- [ ] Observer quels events sont acceptés vs ignorés (Spark UI)
- [ ] Implémenter le routage des late events vers le topic `late_listening_events`
- [ ] Vérifier dans Kafka UI que les late events arrivent dans le bon topic

#### Critère de validation
Spark UI → Streaming tab → Events delay histogram montre des events tardifs identifiés.

---

### Issue #16 — Exactly-once semantics bout-en-bout

**Labels :** `phase-2` `spark` `kafka` `jeudi`
**Milestone :** Phase 2 — Streaming

#### Contexte
Configurer la chaîne exactly-once complète : producteur Kafka → Kafka broker → Spark → sinks.

#### Ce qu'on attend

- [ ] Producteur : `enable.idempotence=True`, `acks='all'`, `transactional.id='p2p-simulator-1'`
- [ ] Consommateur Spark : `isolation.level=read_committed` dans les options Kafka
- [ ] Vérifier : arrêter le job Spark pendant 2 minutes, le relancer → aucun doublon dans PostgreSQL
- [ ] Comparer `COUNT(DISTINCT event_id)` avant/après redémarrage
- [ ] Documenter la procédure de vérification dans `docs/RUNBOOK.md`

#### Critère de validation
`SELECT COUNT(*) - COUNT(DISTINCT event_id) AS doublons FROM listening_events` → 0.

---

### Issue #17 — Job streaming_enrichment_job

**Labels :** `phase-2` `spark` `jeudi`
**Milestone :** Phase 2 — Streaming

#### Contexte
Créer `spark_jobs/streaming_enrichment_job.py` pour enrichir les events listening avec le catalogue et les events P2P.

#### Ce qu'on attend

- [ ] Jointure stream-static : `listening_events` × catalogue PostgreSQL (track_id → artiste, genre)
- [ ] Jointure stream-stream : `listening_events` × `p2p_network_events` (qui a servi quoi à qui)
- [ ] Gérer le watermark de la fenêtre de jointure stream-stream (2 minutes max)
- [ ] Déduplication avec `dropDuplicates(["event_id"])` + watermark
- [ ] Écriture dans topic Kafka `enriched_events`
- [ ] Écriture en Parquet sur MinIO (partitionné par `date/hour`)

#### Critère de validation
Topic `enriched_events` dans Kafka UI contient des events avec les champs artiste et genre enrichis.

---

### Issue #18 — Job fraud_detection_job

**Labels :** `phase-2` `spark` `jeudi`
**Milestone :** Phase 2 — Streaming

#### Contexte
Créer `spark_jobs/fraud_detection_job.py` pour détecter les patterns frauduleux en temps réel.

#### Ce qu'on attend

- [ ] Activer le mode fraud du simulateur : `--mode fraud`
- [ ] Implémenter **3 règles de détection** basées sur des fenêtres :
  1. Plus de 100 écoutes en 10 min pour un même `user_id`
  2. Durée moyenne < 5 secondes sur une fenêtre de 1 heure
  3. Taux d'échec de transfert P2P > 50% sur 15 min
- [ ] Maintenir un score de suspicion par utilisateur avec `flatMapGroupsWithState`
- [ ] Écrire les alertes dans le topic Kafka `fraud_alerts`
- [ ] Écrire dans `fraud_detections` PostgreSQL ET dans `dead_letter_events`

#### Critère de validation
Kafka UI → topic `fraud_alerts` contient des alertes pendant que le simulateur tourne en mode `fraud`.

---

### Issue #19 — DAG reconciliation_pipeline

**Labels :** `phase-2` `airflow` `jeudi`
**Milestone :** Phase 2 — Streaming

#### Contexte
Créer le pont batch ↔ streaming : un DAG Airflow qui compare les agrégats batch et temps réel.

#### Ce qu'on attend

Créer `dags/reconciliation_pipeline.py` :
- [ ] Comparer `daily_streams` (batch) vs `realtime_top_tracks` (streaming) pour la même période
- [ ] Calculer le taux de divergence par track (batch_count - streaming_count)
- [ ] Générer un rapport de réconciliation (log + table `reconciliation_reports` à créer)
- [ ] Alerter si divergence > 5% pour un track (on_failure_callback ou XCom)

#### Critère de validation
Log du DAGRun montrant les taux de convergence batch/streaming.

---

### Issue #20 — DAG late_events_reprocessing

**Labels :** `phase-2` `airflow` `jeudi`
**Milestone :** Phase 2 — Streaming

#### Contexte
Créer `dags/late_events_reprocessing.py` pour retraiter périodiquement les events trop tardifs routés par Spark vers `late_listening_events`.

#### Ce qu'on attend

- [ ] Consommer le topic Kafka `late_listening_events` (mode `availableNow`)
- [ ] Revalider les events tardifs
- [ ] Insérer dans `listening_events` si valides
- [ ] Recalculer les agrégats affectés dans `daily_streams`
- [ ] Planification : `@hourly`

#### Critère de validation
Après injection de late events par le simulateur, `SELECT COUNT(*) FROM listening_events` augmente lors de l'exécution du DAG.

---

## PHASE 3 — Interconnexion inter-groupes (Issues #21 à #25)

---

### Issue #21 — Data contracts inter-groupes

**Labels :** `phase-3` `inter-groupes` `vendredi`
**Milestone :** Phase 3 — Interconnexion

#### Contexte
Définir les formats communs pour les topics partagés entre groupes.

#### Ce qu'on attend

- [ ] Compléter `contracts/catalog_federation_schema.json` : format d'un track fédéré
- [ ] Compléter `contracts/p2p_cross_request_schema.json` : format d'une requête cross-group
- [ ] Compléter `contracts/global_metrics_schema.json` : format des métriques partagées
- [ ] Se concerter avec les autres groupes sur les formats (réunion ambassadeurs)
- [ ] Documenter les décisions dans `docs/DATA_CONTRACTS.md`

#### Critère de validation
Tous les groupes ont signé les mêmes schemas (même format, même version).

---

### Issue #22 — DAG catalog_federation_pipeline

**Labels :** `phase-3` `inter-groupes` `vendredi`
**Milestone :** Phase 3 — Interconnexion

#### Ce qu'on attend

- [ ] Créer `dags/catalog_federation_pipeline.py`
- [ ] Publier vos nouveaux tracks dans le topic partagé `catalog_federation`
- [ ] Consommer les catalogues des autres groupes
- [ ] Valider les schemas reçus (DLQ si non conforme)
- [ ] Insérer dans `federated_catalog`

#### Critère de validation
`SELECT source_group, COUNT(*) FROM federated_catalog GROUP BY source_group` → montre les tracks de tous les groupes.

---

### Issue #23 — P2P cross-group

**Labels :** `phase-3` `inter-groupes` `vendredi`
**Milestone :** Phase 3 — Interconnexion

#### Ce qu'on attend

- [ ] Configurer `kafka/cross_group_config.yml` avec les endpoints des autres groupes
- [ ] Le simulateur publie dans `p2p_cross_requests` quand un morceau n'est disponible qu'ailleurs
- [ ] Handler qui consomme les requêtes cross-group entrantes et y répond
- [ ] Métriques de transfert cross-group collectées

#### Critère de validation
Log d'un transfert réussi : `[CROSS-GROUP] Groupe-A → Groupe-B : track_id=... OK`

---

### Issue #24 — Top 50 Global SPOTIFY

**Labels :** `phase-3` `inter-groupes` `vendredi`
**Milestone :** Phase 3 — Interconnexion

#### Ce qu'on attend

- [ ] Chaque groupe publie ses agrégats dans le topic `global_metrics`
- [ ] Job Spark ou DAG Airflow : consommer `global_metrics` de tous les groupes
- [ ] Produire le classement Top 50 Global agrégé
- [ ] Stocker dans Redis (`top50:global`) accessible à tous

#### Critère de validation
`redis-cli get top50:global` → JSON avec les 50 tracks les plus écoutés sur l'ensemble des groupes.

---

### Issue #25 — Chaos engineering + documentation finale

**Labels :** `phase-3` `résilience` `documentation` `vendredi`
**Milestone :** Phase 3 — Interconnexion

#### Ce qu'on attend

- [ ] Scénario chaos 1 : `docker compose stop kafka-2` → le cluster Kafka reste opérationnel
- [ ] Scénario chaos 2 : `docker compose kill spark-master` → Spark redémarre depuis le checkpoint
- [ ] Scénario chaos 3 : `docker compose stop postgres` 2 min → recovery sans perte
- [ ] Documenter les résultats dans `docs/RUNBOOK.md`
- [ ] README final : architecture complète (diagramme + stack + décisions)
- [ ] `pytest tests/ -v` → 0 FAILED

#### Critère de validation
Présentation soutenance Partie 1 : lancer le chaos engineering en live et montrer le recovery automatique.
