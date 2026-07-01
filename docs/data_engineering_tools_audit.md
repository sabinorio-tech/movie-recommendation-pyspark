# Data Engineering Tools and Concepts Audit

**Audit date:** 2026-07-01  
**Scope:** Current repository contents, including application and source code, notebooks and their saved outputs, documentation, dependency and environment files, Docker files, local data directories, model/output directories, tests, and automation/deployment configuration.

## Classification Rules

| Status | Meaning used in this audit |
|---|---|
| **Actively Used** | Implemented in the current application or its required local data-processing path. |
| **Partially Used** | Implemented only as an optional feature, exploratory notebook, manual packaging path, limited check, or otherwise outside the complete current production flow. |
| **Mentioned Only** | Described or reserved in documentation/configuration, but no working implementation or output exists. |
| **Not Used** | No project implementation or project-level usage was found. A dependency merely present inside the local `.venv` is not counted as project usage. |

## Repository-Level Findings

- The interactive application remains a **local, on-demand recommendation flow**: Streamlit calls reusable functions in `src/recommend.py`, PySpark reads `ratings.csv` and `movies.csv`, performs filtering, joins, and aggregation, and converts the small result to Pandas for display.
- A separate batch path in `src/pipeline.py` validates the inputs, creates small processed CSVs, and writes JSON run metadata. `dags/movie_recommender_dag.py` orchestrates its five stages on a daily Airflow schedule with one retry.
- `.github/workflows/ci.yml` checks syntax and required files, runs unit tests, and executes the Spark pipeline against a tiny committed fixture; Docker build validation is available as a manual workflow option.
- The production recommendation logic is a co-rating heuristic. It is **not the notebook's ALS model**.
- `notebooks/01_load_data.ipynb` contains executed exploratory data profiling, an ALS training experiment, predictions, and an RMSE result. No trained model is saved or loaded by the application.
- The raw MovieLens files exist locally under `data/raw/ml-latest/`, but `data/raw/` is ignored by `.gitignore`; `git ls-files` does not list the data. A fresh clone therefore requires a separate dataset download.
- `data/processed/` is an ignored local output zone populated by the batch pipeline; `models/` remains an empty ignored placeholder and no model artifact was found.
- A `Dockerfile` and `.dockerignore` exist and README commands describe a manual image build/run. There is no Docker Compose, container registry workflow, or automated deployment.
- No Kubernetes manifest, Docker Compose file, database integration, cloud deployment configuration, or production monitoring/alerting stack was found.

## Data Engineering Pipeline

| Tool / Concept | Status | Evidence in Repo | How It Is Used | Notes |
|---|---|---|---|---|
| ETL / ELT | **Actively Used** | `src/pipeline.py`; `src/recommend.py`; `README.md` | Extracts local CSVs, transforms/aggregates them with Spark, and loads small CSV/JSON outputs into `data/processed/`. | This is a local filesystem ETL pipeline, not ELT into a warehouse. |
| Batch processing | **Actively Used** | `src/pipeline.py`; `src/recommend.py`; `dags/movie_recommender_dag.py` | Spark processes the finite MovieLens snapshot on demand or as an Airflow batch run. | No streaming engine is present. |
| Apache Spark / PySpark | **Actively Used** | `src/recommend.py`; `requirements.txt`; `notebooks/01_load_data.ipynb` | Provides CSV loading, distributed DataFrames, filtering, joins, grouping, aggregation, sorting, sampling, and exploratory ML. | PySpark is central to the current recommendation path and can be claimed confidently. |
| Orchestration | **Partially Used** | `dags/movie_recommender_dag.py`; `docs/airflow.md`; `requirements-airflow.txt` | Airflow TaskFlow defines five ordered tasks and passes small task results through XCom. | Working local/demo orchestration, separate from the Streamlit runtime; not a production Airflow deployment. |
| Scheduling | **Partially Used** | `dags/movie_recommender_dag.py` | DAG is configured with `schedule="@daily"`, `catchup=False`, and one active run. | Requires a running local Airflow scheduler; GitHub Actions itself is event/manual, not scheduled. |
| Modular pipeline design | **Actively Used** | `src/pipeline.py`; `src/recommend.py`; `app/streamlit_app.py`; `dags/movie_recommender_dag.py` | Reusable validation, processing, recommendation, and metadata functions are called by CLI, CI, or Airflow without changing the UI path. | Clear separation of processing, orchestration, and presentation. |
| Data validation | **Actively Used** | `src/pipeline.py`; `tests/test_pipeline.py`; `src/recommend.py` | Fails fast on missing/empty files and required CSV columns; validates Spark DataFrame columns and recommendation input. | It does not yet enforce explicit data types, rating ranges, or row-count thresholds. |
| Data quality checks | **Partially Used** | `src/pipeline.py`; `tests/test_pipeline.py`; `notebooks/01_load_data.ipynb` | Automated file/schema checks and row counts complement exploratory null/distribution checks. | Core checks can fail CI/Airflow, but broader quality rules remain notebook-only. |
| Logging | **Partially Used** | `src/recommend.py` | Uses Python `logging` for TMDB request statuses, warnings, matches, and fallbacks. | No project logging configuration, structured output, persistence, or Spark/application-wide observability. `INFO` messages may not appear without external configuration. |
| Error handling | **Actively Used** | `src/pipeline.py`; `src/recommend.py`; `app/streamlit_app.py` | Pipeline failures raise, write failed-run metadata in direct CLI mode, and are surfaced by Airflow/CI; TMDB and title failures retain their existing handling. | The dashboard still does not catch every Spark/file failure. |
| Retries | **Partially Used** | `dags/movie_recommender_dag.py` | Airflow tasks have one retry after five minutes. | No request-level exponential backoff or selective task retry policy. |
| Pipeline artifacts | **Partially Used** | `src/pipeline.py`; `data/processed/`; `.gitignore` | Writes top-movie CSV, sample-recommendation CSV, and run metadata JSON locally. | Outputs are overwritten and ignored by Git; there is no durable artifact store/version history. |
| Pipeline freshness timestamps | **Partially Used** | `src/pipeline.py` | Metadata records UTC `started_at` and `completed_at`. | Timestamp is available in JSON but not displayed by Streamlit or backed by freshness alerts. |

## Data Sources and Ingestion

| Tool / Concept | Status | Evidence in Repo | How It Is Used | Notes |
|---|---|---|---|---|
| MovieLens / GroupLens dataset | **Actively Used** | `data/raw/ml-latest/`; `README.md`; `src/recommend.py` | Supplies ratings and movie metadata for recommendations. | The app reads only `ratings.csv` and `movies.csv`; other locally present MovieLens CSVs are not used by current code. |
| CSV ingestion | **Actively Used** | `src/recommend.py`; `notebooks/01_load_data.ipynb` | `spark.read.csv(..., header=True, inferSchema=True)` loads ratings and movies. | Required for the current app. |
| API ingestion | **Partially Used** | `src/recommend.py`; `app/streamlit_app.py`; `.env.example` | `requests.get` queries the TMDB search API for poster metadata when an API key is configured. | Optional presentation enrichment, not ingestion into the core analytical dataset or a stored raw zone. |
| Web scraping | **Not Used** | No scraper, HTML parser, Selenium/Playwright dependency, or scraping code found | — | TMDB access uses an API, not scraping. |
| Live data ingestion | **Partially Used** | `src/recommend.py` | TMDB data is fetched on demand during UI use. | No live ratings feed, event stream, CDC, or continuously updated recommendation dataset. |
| Historical data ingestion | **Actively Used** | `data/raw/ml-latest/README.txt`; `src/recommend.py` | Loads a static MovieLens snapshot containing historical rating behavior. | The bundled local snapshot states that its events span 1995–2023 and it was generated in 2023. |
| Incremental updates | **Not Used** | No watermark, checkpoint, merge/upsert, append-only loader, or changed-record logic found | — | Re-reads full source CSVs; no incremental ingestion path exists. |
| Caching | **Actively Used** | `app/streamlit_app.py`; `src/recommend.py` | Streamlit caches the Spark resource, movie-title list, and posters; `lru_cache` caches poster results and the placeholder. | In-memory/process cache only; no persistent cache or invalidation based on source-file changes. |
| Raw data zone | **Actively Used** | `data/raw/ml-latest/`; `src/recommend.py`; `.gitignore` | Raw CSVs are organized under a dedicated path consumed by Spark. | Local filesystem only and ignored by Git. No immutable/object-storage controls. |
| Processed data zone | **Partially Used** | `src/pipeline.py`; `README.md`; `.gitignore`; `.dockerignore` | Batch runs write small CSV and JSON outputs to `data/processed/`. | Local, overwritten, and ignored; not shared or versioned storage. |

## Processing and Transformation

| Tool / Concept | Status | Evidence in Repo | How It Is Used | Notes |
|---|---|---|---|---|
| Pandas | **Actively Used** | `src/recommend.py`; `app/streamlit_app.py`; `requirements.txt` | Spark's limited recommendation result is converted with `toPandas()` and iterated by Streamlit. | Used as the UI interchange layer, not for full-dataset processing. |
| Feature engineering | **Actively Used** | `src/recommend.py`; `notebooks/01_load_data.ipynb` | Derives normalized `search_title`, fan cohorts, `fan_rating_count`, rating-count features, and experimental ALS recommendations. | Production features are simple deterministic/aggregate features; ALS features remain experimental. |
| Joins / merges | **Actively Used** | `src/recommend.py`; `notebooks/01_load_data.ipynb` | Joins ratings to fan user IDs and aggregated candidate movies to movie metadata. | Spark joins, not Pandas merges. |
| Aggregation | **Actively Used** | `src/recommend.py`; `notebooks/01_load_data.ipynb` | Groups by `movieId` and counts high ratings; notebook also computes distributions and popularity counts. | Central to recommendation ranking. |
| Standardization / normalization | **Actively Used** | `src/recommend.py` | Lowercases titles, trims input, removes release-year suffixes, and moves trailing articles for TMDB matching. | Title normalization only; there is no general data standardization framework. |
| Deduplication | **Actively Used** | `src/recommend.py` | `.distinct()` produces unique fan user IDs before the ratings join. | Targeted logical deduplication; no full-source duplicate audit. |
| Missing value handling | **Partially Used** | `src/recommend.py`; `app/streamlit_app.py`; `notebooks/01_load_data.ipynb` | Handles missing TMDB fields/posters with `None` and a placeholder; notebook profiles selected nulls. | No remediation or rejection strategy for missing values in the core MovieLens production DataFrames. |
| Schema validation | **Actively Used** | `src/pipeline.py`; `tests/test_pipeline.py`; `notebooks/01_load_data.ipynb` | Validates required CSV header columns before Spark and required DataFrame columns after loading. | Column-presence contract only; types are still inferred and schema evolution is not managed. |

## Automation and CI/CD

| Tool / Concept | Status | Evidence in Repo | How It Is Used | Notes |
|---|---|---|---|---|
| GitHub Actions | **Actively Used** | `.github/workflows/ci.yml` | Runs validation on pushes, pull requests, nightly at `00:00 UTC`, and by manual dispatch. | The nightly job validates the tiny fixture, not the full Git-ignored MovieLens dataset. |
| Scheduled workflows | **Actively Used** | `dags/movie_recommender_dag.py`; `.github/workflows/ci.yml` | Airflow schedules the real local pipeline daily; GitHub Actions schedules lightweight CI nightly. | Both use daily schedules, but they serve different purposes and run in different environments. |
| Workflow artifacts | **Not Used** | No workflow configuration or artifact upload/download step found | — | Distinct from the saved output cells inside the exploratory notebook. |
| Automatic commits | **Not Used** | No workflow/script that commits generated data or code found | — | All repository commits remain manual. |
| Test automation | **Actively Used** | `tests/test_pipeline.py`; `.github/workflows/ci.yml` | Runs three `unittest` validation tests plus a complete Spark fixture pipeline. | No automated dashboard or TMDB network tests. |
| Deployment automation | **Not Used** | `Dockerfile`; `README.md` | — | Docker build/run instructions are manual. There is no build/push/release/deploy workflow. |

## Storage

| Tool / Concept | Status | Evidence in Repo | How It Is Used | Notes |
|---|---|---|---|---|
| Local CSV files | **Actively Used** | `data/raw/ml-latest/*.csv`; `src/recommend.py` | MovieLens ratings and movies are read from local disk. | Files exist in this workspace but are Git-ignored; only ratings and movies are allowed into the Docker build context by `.dockerignore`. |
| JSON files as data storage | **Partially Used** | `src/pipeline.py` | Persists `pipeline_metadata.json` for run status, timestamps, counts, validation results, and output paths. | Local metadata only; no database/catalog. |
| SQLite / database usage | **Not Used** | No SQL/SQLite file, database dependency, connection string, ORM, or database code found | — | No warehouse, relational database, or NoSQL store is implemented. |
| Processed datasets | **Partially Used** | `src/pipeline.py`; `README.md` | Produces `top_movies.csv` and `sample_recommendations.csv`. | Small demo outputs, overwritten per run and not consumed by Streamlit. |
| Model artifacts | **Mentioned Only** | `README.md`; empty `models/`; `.gitignore` | Reserved for future saved trained models. | The notebook's in-memory ALS model is never serialized. |
| Data versioning | **Not Used** | `README.md`; `data/raw/ml-latest/README.txt` | — | The source snapshot has descriptive release metadata, but there is no DVC/lakeFS, checksum manifest, immutable version pin, or dataset lineage/version workflow. `ml-latest` is not a reproducible version pin. |

## Deployment and Runtime

| Tool / Concept | Status | Evidence in Repo | How It Is Used | Notes |
|---|---|---|---|---|
| Streamlit runtime / deployment | **Partially Used** | `app/streamlit_app.py`; `README.md`; `requirements.txt`; `Dockerfile` | Implements and serves the interactive dashboard locally or inside the container. | The app is complete, but no hosted Streamlit/cloud deployment configuration is present. |
| Docker | **Partially Used** | `Dockerfile`; `.dockerignore`; `README.md` | Defines Python 3.11 + Java runtime, installs dependencies, copies allowed sources/data, exposes port 8501, and starts Streamlit. | Real packaging exists and README gives manual commands, but Docker is optional and there is no evidence/configuration of an automated deployed container. README's “Add Docker deployment” roadmap item is stale relative to the Dockerfile. |
| Docker Compose | **Not Used** | No `compose.yml`, `compose.yaml`, or `docker-compose*` file found | — | Single-container manual Docker instructions only. |
| Kubernetes | **Not Used** | No Kubernetes manifest, Helm chart, or Kustomize configuration found | — | Kubernetes is neither required nor implemented. |
| Environment variables | **Partially Used** | `.env.example`; `src/recommend.py`; `app/streamlit_app.py`; `dags/movie_recommender_dag.py` | Configures optional TMDB access plus Airflow project/data/output paths and bounded Spark resources. | Local configuration; no typed centralized settings layer. |
| Secrets management | **Partially Used** | `.env.example`; `.gitignore`; `.dockerignore`; `README.md` | Uses a placeholder template, ignores `.env`, and avoids copying `.env` into the image. | Sensible local secret hygiene, but no managed secret store, CI secret integration, rotation, or validation. |
| Dependency management | **Actively Used** | `requirements.txt`; `requirements-airflow.txt`; `Dockerfile`; `docs/airflow.md` | Keeps app dependencies separate from pinned Airflow 3.2.2 installed with official constraints. | App dependencies remain unpinned; the separation avoids adding Airflow to the Streamlit image. |
| Java runtime | **Actively Used** | `Dockerfile`; PySpark usage in `src/recommend.py` | Docker installs `default-jre-headless`, which PySpark requires in the container. | Local Java setup is assumed rather than managed by the Python requirements file. |
| Cloud deployment | **Mentioned Only** | `README.md` | Listed under future improvements. | No cloud provider, infrastructure-as-code, service configuration, or deployment workflow exists. |
| Airflow | **Partially Used** | `dags/movie_recommender_dag.py`; `requirements-airflow.txt`; `docs/airflow.md` | TaskFlow DAG orchestrates five daily batch stages with dependencies, logs, run history, and one retry. | Real local/demo implementation, not a production deployment; Airflow must point at the repository so `src` is importable. |

## Data Science Integration

| Tool / Concept | Status | Evidence in Repo | How It Is Used | Notes |
|---|---|---|---|---|
| Collaborative-filtering-style recommendation | **Actively Used** | `src/recommend.py`; `README.md` | Finds users who highly rated a selected movie and ranks other highly rated movies from that cohort. | It is a heuristic co-rating method, not a trained collaborative-filtering model. |
| Model training data | **Partially Used** | `notebooks/01_load_data.ipynb` | Samples 5% of ratings and makes an 80/20 split for experimental ALS training/evaluation. | Notebook-only; the production recommendation function does not train a model. |
| Apache Spark MLlib ALS | **Partially Used** | `notebooks/01_load_data.ipynb`; `README.md` | Fits an `ALS` model and generates per-user recommendations in an executed exploratory notebook. | Not imported by `src/recommend.py` or loaded by Streamlit. Some saved predicted ratings exceed the source 0.5–5 scale, showing the experiment needs refinement before production use. |
| Prediction pipeline | **Partially Used** | `notebooks/01_load_data.ipynb` | Calls `model.transform(test)` and `recommendForAllUsers(10)`. | No reusable prediction module, persisted model, scheduled inference, or production serving path. The live app uses the heuristic instead. |
| Model artifacts | **Mentioned Only** | `README.md`; empty `models/` | Saving trained models is a future improvement. | No serialized ALS model, metadata, or model registry entry found. |
| Evaluation outputs | **Partially Used** | `notebooks/01_load_data.ipynb` | Uses `RegressionEvaluator` and stores a saved notebook output of RMSE `1.0691577489673425`. | One exploratory result only; no repeatable evaluation script, baseline comparison, report artifact, or acceptance threshold. |
| Model compatibility / version checks | **Not Used** | `requirements.txt`; `src/recommend.py`; `app/streamlit_app.py` | — | No model is loaded, dependencies are unpinned, and there are no artifact schema/library/version checks. |
| Jupyter notebooks | **Partially Used** | `notebooks/01_load_data.ipynb`; `notebooks/02_test_recommender.ipynb` | Used for EDA, data-quality profiling, ALS experimentation, and a manual recommender call. | Exploratory workflow only; notebook 02 has no execution counts or saved result. Notebooks are excluded from the Docker context. |

## Monitoring and Reliability

| Tool / Concept | Status | Evidence in Repo | How It Is Used | Notes |
|---|---|---|---|---|
| Smoke tests | **Actively Used** | `.github/workflows/ci.yml`; `tests/fixtures/ml-small/` | CI runs the complete Spark batch pipeline and verifies all three output files are non-empty. | Uses 12 ratings rather than the full dataset to remain fast. |
| Unit tests | **Actively Used** | `tests/test_pipeline.py`; `.github/workflows/ci.yml` | Tests valid fixtures, missing files, and rejected missing columns with `unittest`. | Coverage is intentionally narrow and pipeline-focused. |
| Dashboard tests | **Not Used** | `app/streamlit_app.py`; no Streamlit test code found | — | No `streamlit.testing`, browser test, or UI snapshot test. README screenshots are documentation, not tests. |
| API failure handling | **Partially Used** | `src/recommend.py` | Applies an 8-second timeout; handles request exceptions, non-200 responses, invalid JSON, missing results, and missing poster paths. | Solid handling for the optional TMDB feature, but no retry/backoff, circuit breaker, metrics, or alerting. |
| Stale-data handling | **Not Used** | `src/recommend.py`; `data/raw/ml-latest/README.txt` | — | The application does not assess age, reject old files, refresh data, or warn that its 2023 snapshot is stale. |
| Freshness indicators | **Partially Used** | `src/pipeline.py`; `app/streamlit_app.py` | Run timestamps are written to metadata. | Dashboard still does not show dataset release date or last successful pipeline run. |
| Fallback behavior | **Actively Used** | `src/recommend.py`; `app/streamlit_app.py` | App remains usable without TMDB; poster failures use an inline placeholder, and title matching falls back from exact normalized match to substring matching. | Fallback covers optional posters/title matching, not missing CSVs or Spark failures. |
| Operational monitoring / alerting | **Partially Used** | `dags/movie_recommender_dag.py`; `src/pipeline.py` | Airflow supplies local task state/history/logs and metadata records run status. | No metrics, health endpoint, tracing, external alerts, or production monitoring configuration. |

## Strongest Data Engineering Concepts Demonstrated

The following are clearly implemented and can be stated confidently in a presentation or interview:

- Local batch processing of a large historical MovieLens dataset with Apache Spark / PySpark.
- CSV ingestion into Spark DataFrames.
- Spark transformations including filters, derived columns, joins, distinct operations, group-by aggregation, ranking, and result limiting.
- Modular separation between reusable batch/recommendation functions, Airflow orchestration, CI, and the Streamlit presentation layer.
- Airflow TaskFlow orchestration with five explicit stages, a daily schedule, dependencies, run history/logs, and task retries in a local/demo setup.
- Automated GitHub Actions checks for syntax, required files, unit tests, and a full tiny-fixture Spark pipeline.
- Raw and processed local data zones under `data/raw/ml-latest/` and `data/processed/`.
- Machine-readable pipeline metadata with timestamps, status, row counts, schema results, and output paths.
- Pandas integration for moving a small final Spark result into the UI.
- Application caching of the Spark session, reference data, and optional API results.
- Defensive error/fallback handling for optional TMDB poster enrichment.
- Exploratory data-quality profiling, schema inspection, train/test splitting, ALS training, prediction, and RMSE evaluation in a notebook—provided these are explicitly described as experimental.
- Container packaging with Docker and a Java runtime for PySpark—provided this is described as an available manual runtime option, not automated production deployment.

## Concepts Mentioned but Not Implemented

- **Saved model artifacts:** `README.md` and `models/` reserve the idea, but the ALS model is not serialized.
- **Production ALS recommendations:** the roadmap proposes them; only an exploratory notebook experiment exists.
- **Cloud deployment:** listed as a future improvement with no provider or deployment configuration.
- **Docker deployment automation:** Docker packaging exists, but publishing/deploying the image remains manual. Docker itself should be classified as partially used, not “mentioned only.”

Kubernetes, Docker Compose, databases, automatic image publishing/deployment, data versioning tools, and cloud deployment remain unimplemented. Airflow and GitHub Actions now have real repository implementations, but Airflow is local/demo infrastructure rather than a production service.

## Recommended Wording for Presentation

> “The project uses PySpark for local batch processing of MovieLens CSV data and Streamlit for interactive recommendations. A reusable batch pipeline validates inputs, creates processed CSV outputs, and writes JSON run metadata. Airflow now orchestrates its five stages on a daily local schedule with task dependencies and retries, while GitHub Actions automatically checks syntax, unit tests, required files, and the complete Spark flow against a tiny fixture. Docker packages the Streamlit app. This remains a local/demo architecture: it does not use Kubernetes, a database, shared artifact storage, or cloud deployment.”

## Recommended Wording for CV / LinkedIn

- Built a local batch recommendation workflow with **Apache Spark / PySpark**, ingesting and transforming the MovieLens CSV dataset.
- Implemented Spark DataFrame **filtering, joins, deduplication, feature derivation, group-by aggregation, and ranking** over historical ratings data.
- Orchestrated a five-stage batch pipeline with **Apache Airflow TaskFlow**, including daily scheduling, dependencies, retries, local processed outputs, and JSON run metadata.
- Added **GitHub Actions CI** for syntax checks, unit tests, file contracts, and an end-to-end PySpark run against a small fixture.
- Integrated **PySpark, Pandas, and Streamlit** in a modular recommendation application with cached Spark and reference-data resources.
- Added optional **TMDB REST API** enrichment using Requests, environment-based configuration, response caching, timeouts, and graceful poster fallbacks.
- Explored **Spark MLlib ALS**, train/test splitting, prediction, and **RMSE evaluation** in Jupyter; kept this work explicitly separate from the production heuristic.
- Created a **Docker** runtime definition with Java support for PySpark and Streamlit; CI can validate the build manually, while image publishing/deployment remains manual.
