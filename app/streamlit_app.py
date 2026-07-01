import html
import os
import sys
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.download_data import DatasetDownloadError, ensure_movielens_data
from src.recommend import (
    get_movie_poster,
    get_movie_titles,
    get_placeholder_poster_url,
    get_spark_session,
    recommend_similar_movies,
)


@st.cache_resource
def cached_spark():
    """Return the cached Spark session used by the Streamlit app."""
    return get_spark_session("MovieRecommendationStreamlitApp")


@st.cache_data
def cached_movie_titles(data_path):
    """Return cached movie titles for the search dropdown."""
    spark = cached_spark()
    return get_movie_titles(data_path=data_path, spark=spark)


@st.cache_data(show_spinner=False)
def cached_movie_poster(movie_title):
    """Return a cached poster URL for one movie title."""
    return get_movie_poster(movie_title)


def is_tmdb_configured():
    """Return whether poster lookup is enabled for this Streamlit process."""
    return bool(os.getenv("TMDB_API_KEY"))


def render_recommendation_card(movie):
    """Render one recommendation as a poster card."""
    title = html.escape(str(movie["title"]))
    genres = html.escape(str(movie["genres"]).replace("|", ", "))
    fan_rating_count = int(movie["fan_rating_count"])
    poster_url = (
        cached_movie_poster(str(movie["title"]))
        if is_tmdb_configured()
        else None
    )
    poster_url = poster_url or get_placeholder_poster_url()
    poster_url = html.escape(poster_url, quote=True)

    st.markdown(
        f"""
        <div class="movie-card">
            <img src="{poster_url}" alt="Poster for {title}" />
            <div class="movie-card-body">
                <h3>{title}</h3>
                <p class="genres">{genres}</p>
                <p class="count">{fan_rating_count:,} high ratings from similar fans</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.set_page_config(
    page_title="Movie Recommender",
    page_icon="🎬",
    layout="wide",
)

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 2rem;
        }

        .movie-card {
            background: #111827;
            border: 1px solid #263244;
            border-radius: 8px;
            box-shadow: 0 12px 28px rgba(0, 0, 0, 0.24);
            color: #f9fafb;
            height: 100%;
            margin-bottom: 1rem;
            overflow: hidden;
        }

        .movie-card img {
            aspect-ratio: 2 / 3;
            background: #1f2937;
            display: block;
            object-fit: cover;
            width: 100%;
        }

        .movie-card-body {
            padding: 0.8rem;
        }

        .movie-card h3 {
            font-size: 1rem;
            line-height: 1.25;
            margin: 0 0 0.5rem;
        }

        .movie-card p {
            margin: 0;
        }

        .movie-card .genres {
            color: #cbd5e1;
            font-size: 0.84rem;
            min-height: 2.4rem;
        }

        .movie-card .count {
            color: #93c5fd;
            font-size: 0.78rem;
            font-weight: 700;
            margin-top: 0.7rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🎬 Movie Recommendation System")
st.write("Find movies liked by users with similar taste.")

if not is_tmdb_configured():
    st.warning(
        "TMDB API key not configured. Add TMDB_API_KEY to your local .env "
        "file or to Streamlit Cloud secrets to enable posters."
    )

try:
    active_data_path = ensure_movielens_data()
except DatasetDownloadError as error:
    st.error(str(error))
    st.stop()

try:
    spark = cached_spark()
except Exception as error:
    st.error(
        "Spark could not start. The deployment needs Java 17 and enough memory "
        "for the configured Spark driver. Check the Streamlit Cloud build logs."
    )
    st.exception(error)
    st.stop()

movie_titles = cached_movie_titles(active_data_path)

selected_movie = st.selectbox(
    "Choose a movie you like:",
    movie_titles,
)

top_n = st.slider(
    "Number of recommendations:",
    min_value=5,
    max_value=20,
    value=10,
)

if st.button("Recommend"):
    with st.spinner("Finding recommendations..."):
        try:
            recommendations = recommend_similar_movies(
                selected_movie,
                top_n=top_n,
                data_path=active_data_path,
                spark=spark,
            )

            st.subheader(f"Because you liked: {selected_movie}")

            if recommendations.empty:
                st.warning("No recommendations found.")
            else:
                recommendations_per_row = 5

                for start in range(0, len(recommendations), recommendations_per_row):
                    row = recommendations.iloc[start:start + recommendations_per_row]
                    columns = st.columns(recommendations_per_row)

                    for column, (_, movie) in zip(columns, row.iterrows()):
                        with column:
                            render_recommendation_card(movie)

        except ValueError as error:
            st.error(str(error))
