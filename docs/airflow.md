# Running the Movie Recommender Pipeline with Airflow

Airflow orchestrates the batch pipeline in `src/pipeline.py`. Spark still does
the data processing; Airflow supplies scheduling, task dependencies, retries,
run history, and task logs.

## DAG Tasks

The `movie_recommender_batch_pipeline` DAG runs daily and has five sequential
tasks:

1. `check_raw_data_exists`
2. `validate_input_schema`
3. `run_spark_processing`
4. `generate_sample_recommendations`
5. `write_pipeline_metadata`

Each task has one retry with a five-minute delay. Catchup is disabled, so a new
local Airflow installation does not replay historical daily runs.

## Outputs

A successful run creates these ignored local artifacts in `data/processed/`:

- `top_movies.csv`
- `sample_recommendations.csv`
- `pipeline_metadata.json`

The metadata records UTC timestamps, status, input file details, row counts,
schema-validation results, and output paths.

## Install Airflow Locally

Airflow is deliberately separate from `requirements.txt` so it does not bloat
the Streamlit Docker image. From the repository root, create a dedicated Python
3.11 environment:

```bash
python3.11 -m venv .venv-airflow
source .venv-airflow/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -r requirements-airflow.txt \
  --constraint \
  https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.11.txt
```

Airflow officially recommends its version-specific constraint file because it
is an application with a large dependency set. Java must also be available for
PySpark (`java -version`).

## Start Airflow Against This Repository

Do **not** copy only the DAG into `~/airflow/dags`: the DAG imports
`src/pipeline.py`. Instead, point Airflow's DAG folder at this repository:

```bash
cd /path/to/movie-recommendation-pyspark
source .venv-airflow/bin/activate

export MOVIE_RECOMMENDER_PROJECT_ROOT="$PWD"
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"

airflow standalone
```

Keep that terminal open. The command prints the local UI URL and administrator
credentials. Open the UI, find `movie_recommender_batch_pipeline`, enable it,
and trigger a run. The same environment variables must be present whenever the
Airflow services are restarted.

### Airflow 3 Login

The local Airflow 3 `SimpleAuthManager` user is `admin`, but its password is
randomly generated; it is not necessarily `admin`. Display the current local
password from a second terminal:

```bash
cd /path/to/movie-recommendation-pyspark
export AIRFLOW_HOME="$PWD/.airflow"
python -c 'import json, os; p=os.path.join(os.environ["AIRFLOW_HOME"], "simple_auth_manager_passwords.json.generated"); print(json.load(open(p, encoding="utf-8"))["admin"])'
```

Use `admin` plus the printed value in the UI. Keep that generated password file
private. If the browser retained credentials from a previous Airflow instance,
log out or use a private/incognito window.

If Airflow itself runs elsewhere, set these paths to locations visible from
that Airflow worker:

```bash
export MOVIE_RECOMMENDER_PROJECT_ROOT=/absolute/path/to/movie-recommendation-pyspark
export MOVIE_RECOMMENDER_DATA_PATH=/absolute/path/to/data/raw/ml-latest
export MOVIE_RECOMMENDER_OUTPUT_PATH=/absolute/path/to/data/processed
export SAMPLE_MOVIE_TITLE="Toy Story"
```

## Command-Line Checks

Validate the real input files without starting Spark:

```bash
python -m src.pipeline validate --data-path data/raw/ml-latest
```

Run the full pipeline without Airflow:

```bash
python -m src.pipeline run \
  --data-path data/raw/ml-latest \
  --output-path data/processed \
  --movie-title "Toy Story"
```

Run the complete pipeline quickly against the committed CI fixture:

```bash
SPARK_LOCAL_IP=127.0.0.1 \
SPARK_DRIVER_MEMORY=1g \
SPARK_EXECUTOR_MEMORY=1g \
SPARK_SHUFFLE_PARTITIONS=2 \
python -m src.pipeline run \
  --data-path tests/fixtures/ml-small \
  --output-path /tmp/movie-recommender-output \
  --movie-title "Toy Story" \
  --top-n 3 \
  --processed-top-n 5
```

After Airflow is running, verify that it can parse the DAG:

```bash
airflow dags list | grep movie_recommender_batch_pipeline
airflow dags list-import-errors
```

Execute one DAG run from the CLI if desired:

```bash
airflow dags test movie_recommender_batch_pipeline 2026-07-01
```

## Scope and Limitations

- This is a local/demo Airflow setup, not a production Airflow deployment.
- The daily DAG processes the full local CSV snapshot; it is not incremental.
- The metadata/output files are local and overwritten by each successful run.
- Direct CLI failures write failed-run metadata; if an upstream Airflow task
  fails, Airflow records that failure and the final metadata task does not run.
- Airflow is not included in the Streamlit app container and no Airflow Docker
  Compose stack is provided.
- For production, Airflow workers need shared/object storage, a production
  metadata database, managed secrets, monitoring, and an intentional executor.
