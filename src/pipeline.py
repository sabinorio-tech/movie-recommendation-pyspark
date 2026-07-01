"""Reusable batch pipeline for local runs, Airflow, and CI.

The Streamlit application continues to calculate recommendations on demand.
This module adds a small persisted batch output and run metadata without
changing that interactive code path.
"""

import argparse
import csv
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql.functions import avg, count, desc, round as spark_round

from src.recommend import (
    DEFAULT_DATA_PATH,
    get_spark_session,
    load_movie_lens_data,
    recommend_similar_movies,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "processed"
REQUIRED_COLUMNS = {
    "ratings.csv": {"userId", "movieId", "rating", "timestamp"},
    "movies.csv": {"movieId", "title", "genres"},
}

logger = logging.getLogger(__name__)


def utc_now():
    """Return an ISO 8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def get_pipeline_spark_session(app_name="MovieRecommenderBatchPipeline"):
    """Create a bounded local Spark session suitable for Airflow and CI."""
    return get_spark_session(
        app_name,
        master=os.getenv("SPARK_MASTER", "local[2]"),
        driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "4g"),
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
        shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")),
    )


def check_raw_data_exists(data_path=DEFAULT_DATA_PATH):
    """Fail when either required MovieLens input file is missing or empty."""
    data_path = Path(data_path)
    missing = []
    empty = []
    files = {}

    for filename in REQUIRED_COLUMNS:
        path = data_path / filename
        if not path.is_file():
            missing.append(str(path))
            continue
        if path.stat().st_size == 0:
            empty.append(str(path))
            continue
        files[filename] = {
            "path": str(path.resolve()),
            "size_bytes": path.stat().st_size,
        }

    if missing:
        raise FileNotFoundError(
            "Required MovieLens files are missing: " + ", ".join(missing)
        )
    if empty:
        raise ValueError("Required MovieLens files are empty: " + ", ".join(empty))

    return {"data_path": str(data_path.resolve()), "files": files}


def _read_csv_header(path):
    with Path(path).open("r", encoding="utf-8-sig", newline="") as csv_file:
        return next(csv.reader(csv_file), [])


def validate_input_schema(data_path=DEFAULT_DATA_PATH):
    """Validate required columns from CSV headers without starting Spark."""
    data_path = Path(data_path)
    check_raw_data_exists(data_path)
    schemas = {}

    for filename, required_columns in REQUIRED_COLUMNS.items():
        columns = _read_csv_header(data_path / filename)
        missing_columns = sorted(required_columns.difference(columns))
        if missing_columns:
            raise ValueError(
                f"{filename} is missing required columns: "
                + ", ".join(missing_columns)
            )
        schemas[filename] = columns

    return {"status": "valid", "schemas": schemas}


def _validate_spark_columns(ratings, movies):
    frame_columns = {
        "ratings.csv": set(ratings.columns),
        "movies.csv": set(movies.columns),
    }
    for filename, required_columns in REQUIRED_COLUMNS.items():
        missing = sorted(required_columns.difference(frame_columns[filename]))
        if missing:
            raise ValueError(
                f"Spark DataFrame for {filename} is missing: " + ", ".join(missing)
            )


def run_spark_processing(
    data_path=DEFAULT_DATA_PATH,
    output_path=DEFAULT_OUTPUT_PATH,
    top_n=100,
    spark=None,
):
    """Create a small top-movies aggregate from the full ratings dataset."""
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    owns_spark = spark is None
    spark = spark or get_pipeline_spark_session("MovieRecommenderBatchPipeline")

    try:
        ratings, movies = load_movie_lens_data(data_path=data_path, spark=spark)
        _validate_spark_columns(ratings, movies)

        ratings_count = ratings.count()
        movies_count = movies.count()
        top_movies = (
            ratings.groupBy("movieId")
            .agg(
                count("*").alias("rating_count"),
                spark_round(avg("rating"), 3).alias("average_rating"),
            )
            .join(movies, on="movieId", how="inner")
            .select(
                "movieId",
                "title",
                "genres",
                "rating_count",
                "average_rating",
            )
            .orderBy(desc("rating_count"), desc("average_rating"), "title")
            .limit(top_n)
        )

        top_movies_file = output_path / "top_movies.csv"
        top_movies_pandas = top_movies.toPandas()
        top_movies_pandas.to_csv(top_movies_file, index=False)

        return {
            "ratings_row_count": ratings_count,
            "movies_row_count": movies_count,
            "processed_row_count": len(top_movies_pandas),
            "top_movies_file": str(top_movies_file.resolve()),
        }
    finally:
        if owns_spark:
            spark.stop()


def generate_sample_recommendations(
    data_path=DEFAULT_DATA_PATH,
    output_path=DEFAULT_OUTPUT_PATH,
    movie_title="Toy Story",
    top_n=10,
    spark=None,
):
    """Persist a small recommendation sample for pipeline demonstrations."""
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    owns_spark = spark is None
    spark = spark or get_pipeline_spark_session(
        "MovieRecommenderSampleRecommendations"
    )

    try:
        recommendations = recommend_similar_movies(
            movie_title,
            top_n=top_n,
            data_path=data_path,
            spark=spark,
        )
        recommendations_file = output_path / "sample_recommendations.csv"
        recommendations.to_csv(recommendations_file, index=False)

        return {
            "source_movie": movie_title,
            "recommendation_count": len(recommendations),
            "recommendations_file": str(recommendations_file.resolve()),
        }
    finally:
        if owns_spark:
            spark.stop()


def write_pipeline_metadata(
    check_result,
    schema_result,
    processing_result,
    recommendation_result,
    output_path=DEFAULT_OUTPUT_PATH,
    status="success",
    started_at=None,
    error=None,
):
    """Write a machine-readable record of one completed pipeline run."""
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    metadata = {
        "status": status,
        "started_at": started_at,
        "completed_at": utc_now(),
        "input_data_path": check_result.get("data_path"),
        "input_files": check_result.get("files", {}),
        "schema_validation": schema_result,
        "row_counts": {
            "ratings": processing_result.get("ratings_row_count"),
            "movies": processing_result.get("movies_row_count"),
            "processed_top_movies": processing_result.get("processed_row_count"),
            "sample_recommendations": recommendation_result.get(
                "recommendation_count"
            ),
        },
        "outputs": {
            "top_movies": processing_result.get("top_movies_file"),
            "sample_recommendations": recommendation_result.get(
                "recommendations_file"
            ),
        },
    }
    if error:
        metadata["error"] = error

    metadata_file = output_path / "pipeline_metadata.json"
    metadata_file.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return {"metadata_file": str(metadata_file.resolve()), **metadata}


def run_pipeline(
    data_path=DEFAULT_DATA_PATH,
    output_path=DEFAULT_OUTPUT_PATH,
    movie_title="Toy Story",
    top_n=10,
    processed_top_n=100,
):
    """Run all stages in one process, reusing a single local Spark session."""
    started_at = utc_now()
    check_result = {}
    schema_result = {}
    processing_result = {}
    recommendation_result = {}
    spark = None

    try:
        check_result = check_raw_data_exists(data_path)
        schema_result = validate_input_schema(data_path)
        spark = get_pipeline_spark_session("MovieRecommenderBatchPipeline")
        processing_result = run_spark_processing(
            data_path=data_path,
            output_path=output_path,
            top_n=processed_top_n,
            spark=spark,
        )
        recommendation_result = generate_sample_recommendations(
            data_path=data_path,
            output_path=output_path,
            movie_title=movie_title,
            top_n=top_n,
            spark=spark,
        )
        return write_pipeline_metadata(
            check_result,
            schema_result,
            processing_result,
            recommendation_result,
            output_path=output_path,
            started_at=started_at,
        )
    except Exception as error:
        write_pipeline_metadata(
            check_result,
            schema_result,
            processing_result,
            recommendation_result,
            output_path=output_path,
            status="failed",
            started_at=started_at,
            error={"type": type(error).__name__, "message": str(error)},
        )
        raise
    finally:
        if spark is not None:
            spark.stop()


def build_parser():
    parser = argparse.ArgumentParser(description="Run the MovieLens batch pipeline.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser(
        "validate", help="Check required files and CSV headers without Spark."
    )
    validate_parser.add_argument("--data-path", type=Path, default=DEFAULT_DATA_PATH)

    run_parser = subparsers.add_parser("run", help="Run the full batch pipeline.")
    run_parser.add_argument("--data-path", type=Path, default=DEFAULT_DATA_PATH)
    run_parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    run_parser.add_argument("--movie-title", default="Toy Story")
    run_parser.add_argument("--top-n", type=int, default=10)
    run_parser.add_argument("--processed-top-n", type=int, default=100)
    return parser


def main():
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args()

    if args.command == "validate":
        result = {
            "file_check": check_raw_data_exists(args.data_path),
            "schema_check": validate_input_schema(args.data_path),
        }
    else:
        result = run_pipeline(
            data_path=args.data_path,
            output_path=args.output_path,
            movie_title=args.movie_title,
            top_n=args.top_n,
            processed_top_n=args.processed_top_n,
        )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
