import html
import os
import sys
from pathlib import Path
from urllib.parse import quote_plus

import streamlit as st
from dotenv import load_dotenv
from pyspark import StorageLevel
from pyspark.sql.functions import col

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.download_data import DatasetDownloadError, ensure_movielens_data
from src.recommend import (
    clean_movie_title,
    extract_release_year,
    get_movie_poster,
    get_movie_titles,
    get_placeholder_poster_url,
    get_spark_session,
    load_movie_lens_data,
    recommend_similar_movies,
)


@st.cache_resource
def cached_spark():
    """Return the cached Spark session used by the Streamlit app."""
    return get_spark_session("MovieRecommendationStreamlitApp")


@st.cache_data
def cached_movie_titles(data_path, data_version):
    """Return cached movie titles for the search dropdown."""
    _, movies = cached_movie_lens_data(data_path, data_version)
    return get_movie_titles(movies=movies)


@st.cache_resource
def cached_movie_lens_data(data_path, data_version):
    """Load each dataset version once and reuse it across searches."""
    del data_version  # The fingerprint is part of the Streamlit cache key.
    ratings, movies = load_movie_lens_data(
        data_path=data_path,
        spark=cached_spark(),
    )
    ratings.persist(StorageLevel.MEMORY_AND_DISK)
    movies.persist(StorageLevel.MEMORY_AND_DISK)
    return ratings, movies


@st.cache_data(show_spinner=False)
def cached_movie_poster(movie_title):
    """Return a cached poster URL for one movie title."""
    return get_movie_poster(movie_title)


@st.cache_data
def cached_movie_metadata(movie_title, data_path, data_version):
    """Return real MovieLens metadata for the selected title."""
    _, movies = cached_movie_lens_data(data_path, data_version)
    match = movies.filter(col("title") == movie_title).limit(1).collect()
    return match[0].asDict() if match else {}


@st.cache_data(show_spinner=False, max_entries=128)
def cached_recommendations(movie_title, top_n, data_path, data_version):
    """Reuse recommendation results for searches already calculated."""
    ratings, movies = cached_movie_lens_data(data_path, data_version)
    return recommend_similar_movies(
        movie_title,
        top_n=top_n,
        data_path=data_path,
        spark=cached_spark(),
        ratings=ratings,
        movies=movies,
    )


@st.cache_data(show_spinner="Counting ratings for the selected movie...")
def cached_selected_rating_count(movie_id, data_path, data_version):
    """Return the real number of ratings for one selected movie ID."""
    ratings, _ = cached_movie_lens_data(data_path, data_version)
    return ratings.filter(col("movieId") == int(movie_id)).count()


def get_data_version(data_path):
    """Return a cache fingerprint that changes when either CSV changes."""
    return tuple(
        (
            filename,
            (Path(data_path) / filename).stat().st_size,
            (Path(data_path) / filename).stat().st_mtime_ns,
        )
        for filename in ("ratings.csv", "movies.csv")
    )


def format_movie_title(movie_title):
    """Display leading articles naturally while retaining the release year."""
    release_year = extract_release_year(movie_title)
    normalized_title = clean_movie_title(movie_title)
    return (
        f"{normalized_title} ({release_year})"
        if release_year
        else normalized_title
    )


def is_tmdb_configured():
    """Return whether poster lookup is enabled for this Streamlit process."""
    return bool(os.getenv("TMDB_API_KEY"))


def render_recommendation_card(movie, rank):
    """Render one ranked recommendation using only real result fields."""
    raw_title = str(movie["title"])
    title = html.escape(format_movie_title(raw_title))
    tmdb_url = (
        "https://www.themoviedb.org/search/movie?query="
        f"{quote_plus(clean_movie_title(raw_title))}"
    )
    genres = html.escape(str(movie["genres"]).replace("|", " · "))
    fan_rating_count = int(movie["fan_rating_count"])
    poster_url = (
        cached_movie_poster(str(movie["title"]))
        if is_tmdb_configured()
        else None
    )
    poster_url = html.escape(
        poster_url or get_placeholder_poster_url(), quote=True
    )

    st.markdown(
        f"""
        <article class="movie-card">
            <div class="poster-wrap">
                <span class="rank-badge">{rank}</span>
                <img src="{poster_url}" alt="Poster for {title}" />
            </div>
            <div class="movie-card-body">
                <h3><a href="{tmdb_url}" target="_blank"
                    rel="noopener noreferrer">{title}</a></h3>
                <p class="genres">{genres}</p>
                <p class="fan-count">
                    <span>★</span> {fan_rating_count:,} similar-fan ratings
                </p>
            </div>
        </article>
        """,
        unsafe_allow_html=True,
    )


def find_full_title(movie_titles, short_title):
    """Find a MovieLens title for one compact popular-search label."""
    normalized = short_title.casefold()
    return next(
        (
            title
            for title in movie_titles
            if clean_movie_title(title).casefold() == normalized
        ),
        None,
    )


st.set_page_config(
    page_title="Movie Recommender",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        :root {
            --bg: #050b16;
            --panel: #0d1525;
            --panel-soft: #10192b;
            --border: #26334a;
            --text: #f4f6fb;
            --muted: #a9b3c7;
            --purple: #7657ff;
            --purple-soft: #a363ff;
            --green: #46d36b;
            --yellow: #ffc329;
        }

        .stApp {
            background:
                radial-gradient(circle at 62% -10%, rgba(66, 77, 135, 0.13), transparent 31%),
                linear-gradient(135deg, #07101f 0%, var(--bg) 55%, #070c16 100%);
            color: var(--text);
        }

        [data-testid="stHeader"] { background: transparent; }
        [data-testid="stToolbar"] { right: 1rem; }

        .block-container {
            max-width: 1500px;
            padding: 1.8rem 2rem 2rem;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0b1425 0%, #08101e 100%);
            border-right: 1px solid #19253a;
            min-width: 218px;
        }

        [data-testid="stSidebar"] > div:first-child {
            padding: 1.2rem 1rem;
        }

        .brand {
            align-items: center;
            border-bottom: 1px solid #202c42;
            display: flex;
            flex-direction: column;
            margin-bottom: 1.1rem;
            padding: .35rem 0 1.25rem;
            text-align: center;
        }

        .brand-bot {
            align-items: center;
            background: linear-gradient(145deg, #d292ff, #5ca8ff);
            border-radius: 18px;
            box-shadow: 0 0 25px rgba(118, 87, 255, .38);
            display: flex;
            font-size: 2rem;
            height: 58px;
            justify-content: center;
            margin-bottom: .75rem;
            width: 68px;
        }

        .brand strong { font-size: 1rem; }

        .nav-item {
            align-items: center;
            border-radius: 8px;
            color: #b8c2d5;
            display: flex;
            font-size: .9rem;
            gap: .75rem;
            margin: .26rem 0;
            padding: .68rem .72rem;
        }

        .nav-item.active {
            background: linear-gradient(90deg, #5261a5, #694193);
            color: white;
            font-weight: 700;
        }

        .nav-icon { font-size: 1.05rem; width: 1.25rem; }

        .side-about {
            background: rgba(14, 23, 39, .82);
            border: 1px solid var(--border);
            border-radius: 10px;
            color: var(--muted);
            font-size: .72rem;
            line-height: 1.65;
            margin-top: 2rem;
            padding: 1rem;
        }

        .side-about strong { color: var(--text); font-size: .78rem; }
        .side-fact { margin-top: .55rem; }

        .page-kicker {
            color: var(--muted);
            font-size: 1rem;
            margin: .15rem 0 1.5rem;
        }

        .page-kicker .blue { color: #7092ff; }
        .page-kicker .purple { color: #a36bff; }

        h1.app-title {
            font-size: 2rem;
            letter-spacing: -.035em;
            margin: 0;
        }

        .status-card {
            background: rgba(13, 21, 37, .9);
            border: 1px solid #243149;
            border-radius: 11px;
            color: var(--muted);
            float: right;
            font-size: .72rem;
            line-height: 1.6;
            min-width: 132px;
            padding: .7rem 1rem;
        }

        .healthy { color: var(--green); font-weight: 700; }

        .panel {
            background: linear-gradient(145deg, rgba(15, 24, 41, .98), rgba(10, 18, 31, .98));
            border: 1px solid var(--border);
            border-radius: 11px;
            box-shadow: 0 14px 35px rgba(0, 0, 0, .13);
            padding: 1.2rem 1.25rem;
        }

        [data-testid="stVerticalBlockBorderWrapper"] {
            background: linear-gradient(145deg, rgba(15, 24, 41, .98), rgba(10, 18, 31, .98));
            border-color: var(--border);
            border-radius: 11px;
            padding: .35rem .4rem;
        }

        .section-title {
            color: var(--text);
            font-size: 1.08rem;
            font-weight: 750;
            margin: 0 0 .25rem;
        }

        .section-title.accent {
            border-left: 3px solid #875cff;
            padding-left: .7rem;
        }

        .section-subtitle {
            color: var(--muted);
            font-size: .78rem;
            margin-bottom: .8rem;
        }

        div[data-testid="stSelectbox"] label {
            color: var(--text);
            font-size: 1rem;
            font-weight: 700;
        }

        div[data-baseweb="select"] > div {
            background: #111a2c;
            border-color: #35425d;
            min-height: 3.1rem;
        }

        .stButton > button, .stLinkButton > a {
            background: linear-gradient(135deg, #6847eb, #7657ff);
            border: 1px solid #805fff;
            border-radius: 8px;
            color: white;
            font-weight: 700;
            min-height: 3.05rem;
            transition: .15s ease;
            width: 100%;
        }

        .stButton > button:hover, .stLinkButton > a:hover {
            border-color: #a98eff;
            box-shadow: 0 0 18px rgba(118, 87, 255, .25);
            color: white;
            transform: translateY(-1px);
        }

        .popular-label { color: #d7ddeb; font-size: .75rem; margin-top: .7rem; }

        div[data-testid="stHorizontalBlock"] .popular-button button {
            background: #151e30;
            border-color: transparent;
            color: #b9c3d6;
            font-size: .7rem;
            min-height: 1.9rem;
            padding: .2rem .6rem;
        }

        .recommendation-heading {
            align-items: end;
            display: flex;
            justify-content: space-between;
            margin: 1.4rem .1rem .75rem;
        }

        .movie-card {
            background: linear-gradient(180deg, #111a2a, #0c1422);
            border: 1px solid #26344b;
            border-radius: 9px;
            height: 100%;
            min-height: 390px;
            overflow: hidden;
            position: relative;
        }

        .poster-wrap { position: relative; }

        .movie-card img {
            aspect-ratio: 2 / 3;
            background: #192235;
            display: block;
            object-fit: cover;
            width: 100%;
        }

        .rank-badge {
            align-items: center;
            background: linear-gradient(135deg, #744df6, #bf50f0);
            border-radius: 6px;
            box-shadow: 0 3px 12px rgba(63, 35, 171, .5);
            display: flex;
            font-size: .8rem;
            font-weight: 800;
            height: 28px;
            justify-content: center;
            left: 6px;
            position: absolute;
            top: 6px;
            width: 28px;
            z-index: 2;
        }

        .movie-card-body { padding: .7rem .65rem .8rem; }

        .movie-card h3 {
            font-size: .79rem;
            line-height: 1.35;
            margin: 0 0 .42rem;
            min-height: 2.1rem;
        }

        .movie-card h3 a {
            color: var(--text);
            text-decoration: none;
        }

        .movie-card h3 a:hover {
            color: #a98eff;
            text-decoration: underline;
            text-underline-offset: 3px;
        }

        .movie-card p { margin: 0; }

        .movie-card .genres {
            color: #99a5bb;
            font-size: .64rem;
            line-height: 1.35;
            min-height: 1.75rem;
        }

        .fan-count {
            color: #bac4d7;
            font-size: .65rem;
            margin-top: .55rem !important;
        }

        .fan-count span { color: var(--yellow); }

        .empty-state {
            align-items: center;
            border: 1px dashed #33425e;
            border-radius: 10px;
            color: var(--muted);
            display: flex;
            flex-direction: column;
            justify-content: center;
            min-height: 300px;
            padding: 2rem;
            text-align: center;
        }

        .empty-icon { font-size: 2.8rem; margin-bottom: .7rem; }

        .selected-poster {
            aspect-ratio: 16 / 9;
            border: 1px solid #2b3a53;
            border-radius: 7px;
            object-fit: cover;
            width: 100%;
        }

        .selected-name {
            font-size: 1.7rem;
            letter-spacing: -.03em;
            line-height: 1.12;
            margin: .55rem 0 .35rem;
        }

        .selected-meta { color: #bdc6d7; font-size: .78rem; }

        .selected-genres {
            color: #aab5c8;
            font-size: .78rem;
            line-height: 1.6;
            margin: .8rem 0 1rem;
        }

        .stats-grid {
            display: grid;
            gap: .55rem;
            grid-template-columns: 1fr 1fr;
            margin-top: .8rem;
        }

        .stat-box {
            background: #121b2e;
            border-radius: 7px;
            padding: .8rem .4rem;
            text-align: center;
        }

        .stat-value { color: #8872ff; font-size: 1.25rem; font-weight: 800; }
        .stat-value.green { color: var(--green); }
        .stat-label { color: #aab4c8; font-size: .64rem; margin-top: .15rem; }

        .why-grid {
            display: grid;
            gap: 1.15rem;
            grid-template-columns: repeat(4, 1fr);
            margin-top: 1rem;
        }

        .why-item { align-items: center; display: flex; gap: .7rem; }

        .why-icon {
            align-items: center;
            background: rgba(108, 75, 227, .3);
            border-radius: 50%;
            display: flex;
            flex: 0 0 48px;
            font-size: 1.45rem;
            height: 48px;
            justify-content: center;
        }

        .why-item strong { color: #d7ccff; font-size: .72rem; }
        .why-item p { color: #aab5c7; font-size: .67rem; line-height: 1.45; margin: .2rem 0 0; }

        .pipeline {
            align-items: center;
            display: grid;
            gap: 1rem;
            grid-template-columns: 1.5fr repeat(4, 1fr) 1.4fr;
        }

        .pipe-title { font-size: .72rem; font-weight: 700; }
        .pipe-sub { color: #929db2; font-size: .63rem; }
        .pipe-step { align-items: center; color: #b7c0d1; display: flex; font-size: .65rem; gap: .45rem; }

        .check {
            align-items: center;
            background: #23683d;
            border-radius: 50%;
            color: #a7f3ba;
            display: flex;
            height: 22px;
            justify-content: center;
            width: 22px;
        }

        .check.pending { background: #313a4e; color: #aab3c4; }
        .system-info { color: #8f9aaf; font-size: .62rem; text-align: right; }
        .system-info strong { color: #d4dbea; }

        [data-testid="stAlert"] { border-radius: 9px; }

        @media (max-width: 1050px) {
            .why-grid { grid-template-columns: 1fr 1fr; }
            .pipeline { grid-template-columns: 1fr 1fr; }
            .system-info { text-align: left; }
        }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown(
        """
        <div class="brand">
            <div class="brand-bot">🤖</div>
            <strong>Movie Recommender</strong>
        </div>
        <div class="nav-item active"><span class="nav-icon">⌂</span> Home</div>
        <div class="nav-item"><span class="nav-icon">⌕</span> Search Movies</div>
        <div class="nav-item"><span class="nav-icon">♬</span> Recommendations</div>
        <div class="nav-item"><span class="nav-icon">☆</span> Top Rated</div>
        <div class="nav-item"><span class="nav-icon">▥</span> Analytics</div>
        <div class="nav-item"><span class="nav-icon">⚙</span> Settings</div>
        <div class="nav-item"><span class="nav-icon">ⓘ</span> About</div>
        <div class="side-about">
            <strong>About this app</strong><br>
            Recommendations come from real rating patterns in MovieLens and
            are calculated with PySpark.
            <div class="side-fact">▣ Dataset: MovieLens</div>
            <div class="side-fact">♧ Engine: PySpark</div>
            <div class="side-fact">♥ UI: Streamlit</div>
            <div class="side-fact">▧ Posters: TMDB API</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

try:
    active_data_path = ensure_movielens_data()
except DatasetDownloadError as error:
    st.error(str(error))
    st.stop()

data_version = get_data_version(active_data_path)

try:
    spark = cached_spark()
except Exception as error:
    st.error(
        "Spark could not start. The deployment needs Java 17 and enough memory "
        "for the configured Spark driver. Check the Streamlit Cloud build logs."
    )
    st.exception(error)
    st.stop()

movie_titles = cached_movie_titles(active_data_path, data_version)
default_movie = find_full_title(movie_titles, "Inception") or movie_titles[0]

if "movie_search" not in st.session_state:
    st.session_state.movie_search = default_movie
if "recommendations" not in st.session_state:
    st.session_state.recommendations = None
if "recommendation_source" not in st.session_state:
    st.session_state.recommendation_source = None

header_left, header_right = st.columns([5, 1])
with header_left:
    st.markdown(
        """
        <h1 class="app-title">🎬 Movie Recommender</h1>
        <p class="page-kicker">Discover movies you'll love, powered by
        <span class="blue">PySpark</span> &amp;
        <span class="purple">MovieLens</span></p>
        """,
        unsafe_allow_html=True,
    )
with header_right:
    st.markdown(
        """
        <div class="status-card">System Status<br>
        <span class="healthy">● &nbsp;Healthy</span></div>
        """,
        unsafe_allow_html=True,
    )

main_column, detail_column = st.columns([3.25, 1.25], gap="large")

with main_column:
    with st.container(border=True):
        search_column, button_column = st.columns(
            [5.5, 1.2], vertical_alignment="bottom"
        )
        with search_column:
            selected_movie = st.selectbox(
                "Find a movie you like",
                movie_titles,
                key="movie_search",
                format_func=format_movie_title,
                help="Type part of a title to filter MovieLens movies.",
            )
        with button_column:
            search_clicked = st.button(
                "Search", type="primary", use_container_width=True
            )

        st.markdown(
            '<div class="popular-label">Popular searches:</div>',
            unsafe_allow_html=True,
        )
        popular_labels = [
            "Inception",
            "The Matrix",
            "Interstellar",
            "The Dark Knight",
            "Toy Story",
        ]
        popular_titles = [
            (label, find_full_title(movie_titles, label)) for label in popular_labels
        ]
        popular_columns = st.columns(len(popular_titles))
        for column, (label, full_title) in zip(popular_columns, popular_titles):
            if full_title:
                with column:
                    st.button(
                        label,
                        key=f"popular_{label}",
                        on_click=lambda title=full_title: st.session_state.update(
                            movie_search=title
                        ),
                        use_container_width=True,
                    )

    if search_clicked:
        with st.spinner("Finding movies liked by similar fans..."):
            try:
                st.session_state.recommendations = cached_recommendations(
                    selected_movie,
                    10,
                    active_data_path,
                    data_version,
                )
                st.session_state.recommendation_source = selected_movie
            except ValueError as error:
                st.error(str(error))
                st.session_state.recommendations = None

    recommendations = st.session_state.recommendations
    source_movie = st.session_state.recommendation_source

    st.markdown(
        """
        <div class="recommendation-heading">
            <div><div class="section-title">✨ Recommended for you</div>
            <div class="section-subtitle">Ranked by high ratings from fans with similar taste</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if recommendations is None:
        st.markdown(
            """
            <div class="empty-state">
                <div class="empty-icon">🎞️</div>
                <strong>Your recommendations will appear here</strong>
                <span>Choose a movie and press Search to start the PySpark engine.</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
    elif recommendations.empty:
        st.warning("No recommendations were found for that movie.")
    else:
        cards_per_row = 5
        for start in range(0, len(recommendations), cards_per_row):
            row = recommendations.iloc[start:start + cards_per_row]
            columns = st.columns(cards_per_row)
            for offset, (column, (_, movie)) in enumerate(zip(columns, row.iterrows())):
                with column:
                    render_recommendation_card(movie, start + offset + 1)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        """
        <div class="panel">
            <div class="section-title">Why these recommendations?</div>
            <div class="section-subtitle">The ranking follows real patterns in how MovieLens users rate films.</div>
            <div class="why-grid">
                <div class="why-item"><div class="why-icon">👥</div><div>
                    <strong>Collaborative Filtering</strong><p>Finds users who highly rated your selected movie.</p>
                </div></div>
                <div class="why-item"><div class="why-icon">✨</div><div>
                    <strong>PySpark Engine</strong><p>Processes ratings with distributed DataFrame operations.</p>
                </div></div>
                <div class="why-item"><div class="why-icon">📊</div><div>
                    <strong>MovieLens Data</strong><p>Uses anonymized ratings from real movie fans.</p>
                </div></div>
                <div class="why-item"><div class="why-icon">🎞️</div><div>
                    <strong>Live Results</strong><p>Calculates a fresh ranking for each selected title.</p>
                </div></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with detail_column:
    metadata = cached_movie_metadata(
        selected_movie,
        active_data_path,
        data_version,
    )
    selected_title = html.escape(format_movie_title(selected_movie))
    selected_poster = (
        cached_movie_poster(selected_movie) if is_tmdb_configured() else None
    ) or get_placeholder_poster_url()
    selected_poster = html.escape(selected_poster, quote=True)
    release_year = extract_release_year(selected_movie) or "Year unavailable"
    selected_genres = html.escape(
        str(metadata.get("genres") or "Genres unavailable").replace("|", " · ")
    )
    selected_rating_count = None
    if (
        source_movie == selected_movie
        and metadata.get("movieId") is not None
    ):
        selected_rating_count = cached_selected_rating_count(
            metadata["movieId"],
            active_data_path,
            data_version,
        )
    rating_summary = (
        f"{selected_rating_count:,} ratings"
        if selected_rating_count is not None
        else "Search to calculate rating total"
    )
    tmdb_query = quote_plus(clean_movie_title(selected_movie))

    st.markdown(
        f"""
        <div class="panel">
            <div class="section-title accent">Selected Movie</div>
            <br>
            <img class="selected-poster" src="{selected_poster}" alt="Poster for {selected_title}">
            <h2 class="selected-name">{selected_title}</h2>
            <div class="selected-meta">{release_year} &nbsp;•&nbsp; {rating_summary}</div>
            <div class="selected-genres">{selected_genres}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.link_button(
        "View Details on TMDB ↗",
        f"https://www.themoviedb.org/search/movie?query={tmdb_query}",
        use_container_width=True,
    )

    if recommendations is not None and not recommendations.empty:
        total_signals = int(recommendations["fan_rating_count"].sum())
        represented_genres = {
            genre
            for value in recommendations["genres"].dropna()
            for genre in str(value).split("|")
            if genre != "(no genres listed)"
        }
        result_count = len(recommendations)
        poster_status = "On" if is_tmdb_configured() else "Off"
    else:
        total_signals = 0
        represented_genres = set()
        result_count = 0
        poster_status = "On" if is_tmdb_configured() else "Off"

    st.markdown(
        f"""
        <br>
        <div class="panel">
            <div class="section-title accent">Current Results</div>
            <div class="stats-grid">
                <div class="stat-box"><div class="stat-value">{result_count}</div>
                    <div class="stat-label">Recommendations</div></div>
                <div class="stat-box"><div class="stat-value">{total_signals:,}</div>
                    <div class="stat-label">Fan-rating signals</div></div>
                <div class="stat-box"><div class="stat-value green">{len(represented_genres)}</div>
                    <div class="stat-label">Genres represented</div></div>
                <div class="stat-box"><div class="stat-value green">{poster_status}</div>
                    <div class="stat-label">TMDB posters</div></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

recommendations_ready = recommendations is not None
ready_class = "check" if recommendations_ready else "check pending"
ready_mark = "✓" if recommendations_ready else "○"
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    f"""
    <div class="panel pipeline">
        <div><div class="pipe-title">〽 Data Pipeline Status</div>
            <div class="pipe-sub">Live application state</div></div>
        <div class="pipe-step"><span class="check">✓</span> Dataset<br>Ready</div>
        <div class="pipe-step"><span class="check">✓</span> Spark<br>Ready</div>
        <div class="pipe-step"><span class="check">✓</span> Movies<br>Loaded</div>
        <div class="pipe-step"><span class="{ready_class}">{ready_mark}</span> Recommendations<br>{'Generated' if recommendations_ready else 'Pending'}</div>
        <div class="system-info"><strong>System Info</strong><br>
            PySpark {html.escape(spark.version)} &nbsp;•&nbsp; Python {sys.version_info.major}.{sys.version_info.minor}
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)
