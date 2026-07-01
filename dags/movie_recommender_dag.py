"""Airflow orchestration for the Movie Recommender batch pipeline."""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, task


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(
    os.getenv("MOVIE_RECOMMENDER_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT))
).expanduser().resolve()

if not (PROJECT_ROOT / "src" / "pipeline.py").is_file():
    raise RuntimeError(
        "Movie Recommender source code was not found. Point Airflow at the "
        "repository dags directory, or set MOVIE_RECOMMENDER_PROJECT_ROOT "
        "to the repository root."
    )

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline import (  # noqa: E402
    check_raw_data_exists as pipeline_check_raw_data_exists,
    generate_sample_recommendations as pipeline_generate_recommendations,
    run_spark_processing as pipeline_run_spark_processing,
    utc_now,
    validate_input_schema as pipeline_validate_input_schema,
    write_pipeline_metadata as pipeline_write_metadata,
)


DATA_PATH = Path(
    os.getenv(
        "MOVIE_RECOMMENDER_DATA_PATH",
        str(PROJECT_ROOT / "data" / "raw" / "ml-latest"),
    )
)
OUTPUT_PATH = Path(
    os.getenv(
        "MOVIE_RECOMMENDER_OUTPUT_PATH",
        str(PROJECT_ROOT / "data" / "processed"),
    )
)
SAMPLE_MOVIE_TITLE = os.getenv("SAMPLE_MOVIE_TITLE", "Toy Story")


@dag(
    dag_id="movie_recommender_batch_pipeline",
    description="Validate MovieLens data and create small recommendation outputs.",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "movie-recommender",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["movie-recommender", "pyspark", "data-engineering"],
)
def movie_recommender_batch_pipeline():
    """Define the five intentionally small and visible pipeline stages."""

    @task
    def check_raw_data_exists():
        result = pipeline_check_raw_data_exists(DATA_PATH)
        result["pipeline_started_at"] = utc_now()
        return result

    @task
    def validate_input_schema(file_check):
        del file_check  # The argument establishes the upstream dependency.
        return pipeline_validate_input_schema(DATA_PATH)

    @task
    def run_spark_processing(schema_check):
        del schema_check
        return pipeline_run_spark_processing(DATA_PATH, OUTPUT_PATH)

    @task
    def generate_sample_recommendations(processing_result):
        del processing_result
        return pipeline_generate_recommendations(
            DATA_PATH,
            OUTPUT_PATH,
            movie_title=SAMPLE_MOVIE_TITLE,
        )

    @task
    def write_pipeline_metadata(
        file_check,
        schema_check,
        processing_result,
        recommendation_result,
    ):
        return pipeline_write_metadata(
            file_check,
            schema_check,
            processing_result,
            recommendation_result,
            output_path=OUTPUT_PATH,
            started_at=file_check.get("pipeline_started_at"),
        )

    file_check = check_raw_data_exists()
    schema_check = validate_input_schema(file_check)
    processing_result = run_spark_processing(schema_check)
    recommendation_result = generate_sample_recommendations(processing_result)
    write_pipeline_metadata(
        file_check,
        schema_check,
        processing_result,
        recommendation_result,
    )


dag = movie_recommender_batch_pipeline()
