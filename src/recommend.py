import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, lower, regexp_replace

from src.download_data import DEFAULT_DATA_PATH


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/movie"
TMDB_POSTER_BASE_URL = "https://image.tmdb.org/t/p/w500"
YEAR_PATTERN = re.compile(r"\((\d{4})\)\s*$")
TRAILING_ARTICLE_PATTERN = re.compile(r"^(?P<title>.+), (?P<article>The|A|An)$")

logger = logging.getLogger(__name__)


def get_spark_session(
    app_name="MovieRecommendationSystem",
    master=None,
    driver_memory=None,
    executor_memory=None,
    shuffle_partitions=None,
):
    """Create or reuse a local Spark session for MovieLens processing."""
    master = master or os.getenv("SPARK_MASTER", "local[2]")
    driver_memory = driver_memory or os.getenv("SPARK_DRIVER_MEMORY", "512m")
    executor_memory = executor_memory or os.getenv(
        "SPARK_EXECUTOR_MEMORY", driver_memory
    )
    shuffle_partitions = shuffle_partitions or int(
        os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")
    )

    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )


def load_movie_lens_data(data_path=DEFAULT_DATA_PATH, spark=None):
    """Load MovieLens ratings and movies CSV files as Spark DataFrames."""
    spark = spark or get_spark_session()
    data_path = Path(data_path)

    ratings = spark.read.csv(
        str(data_path / "ratings.csv"),
        header=True,
        inferSchema=True,
    )
    movies = spark.read.csv(
        str(data_path / "movies.csv"),
        header=True,
        inferSchema=True,
    )

    return ratings, movies


def get_movie_titles(data_path=DEFAULT_DATA_PATH, spark=None):
    """Return all movie titles for the Streamlit dropdown."""
    _, movies = load_movie_lens_data(data_path=data_path, spark=spark)

    return [
        row["title"]
        for row in movies.select("title").orderBy("title").collect()
    ]


def clean_movie_title(movie_title):
    """Return a TMDB-friendly title without the MovieLens year suffix."""
    title_without_year = YEAR_PATTERN.sub("", movie_title).strip()
    article_match = TRAILING_ARTICLE_PATTERN.match(title_without_year)

    if article_match:
        article = article_match.group("article")
        title = article_match.group("title")
        return f"{article} {title}"

    return title_without_year


def extract_release_year(movie_title):
    """Return the release year from a MovieLens title when one is present."""
    match = YEAR_PATTERN.search(movie_title)
    return match.group(1) if match else None


def is_tmdb_configured():
    """Return whether a TMDB API key is available in the environment."""
    return bool(os.getenv("TMDB_API_KEY") or TMDB_API_KEY)


@lru_cache(maxsize=1)
def get_placeholder_poster_url():
    """Return an inline placeholder poster image."""
    svg = "".join(
        [
            '<svg xmlns="http://www.w3.org/2000/svg" width="500" ',
            'height="750" viewBox="0 0 500 750">',
            '<rect width="500" height="750" fill="#1f2937"/>',
            '<rect x="42" y="42" width="416" height="666" rx="24" ',
            'fill="#111827" stroke="#4b5563" stroke-width="4"/>',
            '<circle cx="250" cy="300" r="72" fill="#374151"/>',
            '<path d="M210 276l96 56-96 56z" fill="#9ca3af"/>',
            '<text x="250" y="478" text-anchor="middle" fill="#f9fafb" ',
            'font-family="Arial, sans-serif" font-size="34" ',
            'font-weight="700">No Poster</text>',
            '<text x="250" y="524" text-anchor="middle" fill="#d1d5db" ',
            'font-family="Arial, sans-serif" font-size="28">Available</text>',
            "</svg>",
        ]
    )

    return f"data:image/svg+xml;charset=utf-8,{quote(svg)}"


def search_tmdb_movies(query_title, api_key, release_year=None):
    """Search TMDB and return result dictionaries for a title query."""
    params = {
        "api_key": api_key,
        "query": query_title,
        "include_adult": "false",
    }

    if release_year:
        params["year"] = release_year

    try:
        response = requests.get(TMDB_SEARCH_URL, params=params, timeout=8)
        logger.info(
            "TMDB search status=%s query=%r year=%r",
            response.status_code,
            query_title,
            release_year,
        )

        if response.status_code != 200:
            logger.warning(
                "TMDB search failed status=%s query=%r response=%r",
                response.status_code,
                query_title,
                response.text[:250],
            )
            return []

        payload = response.json()
    except requests.RequestException as error:
        logger.warning("TMDB request failed for %r: %s", query_title, error)
        return []
    except ValueError as error:
        logger.warning("TMDB returned invalid JSON for %r: %s", query_title, error)
        return []

    results = payload.get("results", [])
    logger.info("TMDB returned %s results for %r", len(results), query_title)
    return results


def get_result_year(movie_result):
    """Return the release year from a TMDB result when available."""
    release_date = movie_result.get("release_date") or ""
    return release_date[:4] if len(release_date) >= 4 else None


def select_best_tmdb_result(
    movie_results,
    query_title,
    release_year=None,
    allow_fallback=False,
):
    """Select the best TMDB result that has a poster path."""
    normalized_query = query_title.casefold()
    results_with_posters = [
        result for result in movie_results if result.get("poster_path")
    ]

    if not results_with_posters:
        logger.info("TMDB results for %r did not include poster paths", query_title)
        return None

    for result in results_with_posters:
        result_titles = {
            str(result.get("title") or "").casefold(),
            str(result.get("original_title") or "").casefold(),
        }

        if (
            normalized_query in result_titles
            and get_result_year(result) == release_year
        ):
            return result

    for result in results_with_posters:
        result_titles = {
            str(result.get("title") or "").casefold(),
            str(result.get("original_title") or "").casefold(),
        }

        if normalized_query in result_titles:
            return result

    if release_year:
        for result in results_with_posters:
            if get_result_year(result) == release_year:
                return result

    if allow_fallback:
        return results_with_posters[0]

    return None


@lru_cache(maxsize=512)
def get_movie_poster(movie_title):
    """Return a TMDB poster URL for a movie title or None when unavailable."""
    api_key = os.getenv("TMDB_API_KEY") or TMDB_API_KEY
    if not api_key:
        logger.warning("TMDB_API_KEY is not configured. Poster lookup skipped.")
        return None

    query_title = clean_movie_title(movie_title)
    release_year = extract_release_year(movie_title)
    search_attempts = [(query_title, None)]

    if release_year:
        search_attempts.append((query_title, release_year))

    all_results = []

    for title_query, year_filter in search_attempts:
        results = search_tmdb_movies(title_query, api_key, year_filter)
        all_results.extend(results)
        best_match = select_best_tmdb_result(
            results,
            title_query,
            release_year=release_year,
        )

        if best_match:
            poster_path = best_match["poster_path"]
            logger.info("TMDB poster found for %r: %s", movie_title, poster_path)
            return f"{TMDB_POSTER_BASE_URL}{poster_path}"

    fallback_match = select_best_tmdb_result(
        all_results,
        query_title,
        release_year=release_year,
        allow_fallback=True,
    )

    if fallback_match:
        poster_path = fallback_match["poster_path"]
        logger.info("TMDB fallback poster found for %r: %s", movie_title, poster_path)
        return f"{TMDB_POSTER_BASE_URL}{poster_path}"

    logger.info("No TMDB poster found for %r", movie_title)
    return None


def recommend_similar_movies(
    movie_title,
    top_n=10,
    min_rating=4.5,
    data_path=DEFAULT_DATA_PATH,
    spark=None,
):
    """Recommend movies liked by users who highly rated the requested movie."""
    if not movie_title or not movie_title.strip():
        raise ValueError("movie_title must not be empty.")

    ratings, movies = load_movie_lens_data(data_path=data_path, spark=spark)
    query = movie_title.strip().lower()

    movies_for_matching = movies.withColumn(
        "search_title",
        lower(regexp_replace(col("title"), r"\s*\(\d{4}\)$", "")),
    )

    exact_matches = movies_for_matching.filter(col("search_title") == query)
    source_movies = exact_matches

    if source_movies.limit(1).count() == 0:
        source_movies = movies_for_matching.filter(lower(col("title")).contains(query))

    source_movie_ids = [
        row["movieId"] for row in source_movies.select("movieId").collect()
    ]

    if not source_movie_ids:
        raise ValueError(f"No movie found for title: {movie_title}")

    fan_user_ids = (
        ratings.filter(
            (col("movieId").isin(source_movie_ids)) & (col("rating") >= min_rating)
        )
        .select("userId")
        .distinct()
    )

    recommendations = (
        ratings.join(fan_user_ids, on="userId")
        .filter(
            (col("rating") >= min_rating) & (~col("movieId").isin(source_movie_ids))
        )
        .groupBy("movieId")
        .agg(count("*").alias("fan_rating_count"))
        .join(movies, on="movieId")
        .select("movieId", "title", "genres", "fan_rating_count")
        .orderBy(desc("fan_rating_count"), "title")
        .limit(top_n)
    )

    return recommendations.toPandas()
