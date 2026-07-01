# Movie Recommendation System

A PySpark and Streamlit application that recommends movies from the MovieLens
dataset. Choose a movie you like, and the app returns other movies that were
highly rated by users with similar taste.

## Project Description

This project builds a movie recommendation system around the MovieLens Latest
Dataset. The current application uses a collaborative filtering approach often
described as "people who liked this also liked": it finds users who gave a high
rating to the selected movie, then ranks other movies those same users also
rated highly.

MovieLens was chosen because it is a well-known recommendation dataset with
real user-rating behavior, consistent movie identifiers, and enough scale to
make PySpark useful during exploration and processing.

Technologies used:

- PySpark for loading, joining, filtering, and aggregating the full dataset.
- Streamlit for the interactive web interface.
- Apache Airflow for daily orchestration of the batch output pipeline.
- GitHub Actions for syntax, unit, fixture-pipeline, and repository checks.
- Pandas for returning recommendation results in a UI-friendly format.
- Requests for optional TMDB poster API calls.
- python-dotenv for loading local configuration from `.env`.

## Features

- Movie search through the Streamlit selection interface.
- Recommendation engine based on similar users' highly rated movies.
- Streamlit interface with recommendation cards.
- Full MovieLens Latest Dataset support.
- ALS experimentation in the exploratory notebook.
- Optional TMDB poster lookup with a safe placeholder fallback.

## Dataset

The project uses the MovieLens Latest Dataset (`ml-latest`) from GroupLens.
The local dataset included in `data/raw/ml-latest` contains:

- 33,832,162 ratings.
- 330,975 users.
- 86,537 movies.

The raw data is intentionally ignored by git because it is large. Download the
dataset from GroupLens and place the extracted files in:

```text
data/raw/ml-latest/
```

Expected files include:

- `ratings.csv`
- `movies.csv`
- `tags.csv`
- `links.csv`
- `genome-scores.csv`
- `genome-tags.csv`

Data source: GroupLens MovieLens datasets. If you use this project in a
publication or portfolio write-up, credit GroupLens and cite the MovieLens
dataset paper referenced in the dataset README.

Movie posters are retrieved from TMDB when `TMDB_API_KEY` is configured. This
project uses TMDB poster metadata but is not endorsed or certified by TMDB.

## Installation

```bash
git clone <repo>
cd <repo>

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

## Environment Variables

Create a `.env` file in the project root:

```bash
cp .env.example .env
```

Then edit `.env` with your TMDB API key:

```env
TMDB_API_KEY=your_tmdb_api_key_here
```

The `.env` file is ignored by git so real API keys are not committed.

## Running the App

```bash
streamlit run app/streamlit_app.py
```

The app works without a poster API key. When no poster is available, it displays
a placeholder image.

To enable movie posters from TMDB, add `TMDB_API_KEY` to `.env` and restart the
Streamlit app.

## Deploying on Streamlit Community Cloud

Deploy `app/streamlit_app.py` with Python 3.12 selected under **Advanced
settings**. The root `packages.txt` installs the Java 17 runtime required by
PySpark. Spark defaults to two local worker threads and a 512 MB driver heap so
it can start within a small cloud container; override these with
`SPARK_MASTER`, `SPARK_DRIVER_MEMORY`, `SPARK_EXECUTOR_MEMORY`, and
`SPARK_SHUFFLE_PARTITIONS` when more resources are available.

To enable posters, add this TOML entry under the app's **Settings > Secrets**:

```toml
TMDB_API_KEY = "your_tmdb_api_key_here"
```

The TMDB key is optional and is unrelated to Spark startup.

The full MovieLens dataset is intentionally excluded from Git and is too large
for the repository. When those files are absent, the app clearly enters demo
mode and uses the tiny committed test fixture. For useful public
recommendations, deploy a cloud-sized sample as `data/raw/ml-latest/ratings.csv`
and `movies.csv`, or adapt the app to read preprocessed recommendations from
external storage. A Docker deployment can use the full local dataset instead.

## Data Engineering Architecture

- **PySpark** reads MovieLens CSVs and performs the filtering, joins,
  deduplication, aggregation, and recommendation processing.
- **Airflow** orchestrates and schedules a daily five-task batch pipeline. It
  validates inputs, invokes Spark, creates small processed outputs, and records
  run metadata; it does not replace Spark.
- **Streamlit** provides the interactive user interface and continues to
  calculate recommendations on demand exactly as before.
- **Docker** packages the Streamlit application, Python dependencies, Java, and
  locally available required MovieLens files into a runnable image.
- **GitHub Actions** validates Python syntax and required files, runs unit tests,
  and executes the real Spark pipeline against a tiny committed fixture. A
  Docker build can be requested manually from the workflow UI.

The layers are intentionally separate: Airflow is not installed in the
Streamlit image, and CI never processes the full 890 MB ratings file.

## Running the Batch Pipeline

Validate input files and headers without starting Spark:

```bash
python -m src.pipeline validate --data-path data/raw/ml-latest
```

Run all batch stages directly:

```bash
python -m src.pipeline run \
  --data-path data/raw/ml-latest \
  --output-path data/processed \
  --movie-title "Toy Story"
```

This writes `top_movies.csv`, `sample_recommendations.csv`, and
`pipeline_metadata.json` under `data/processed/`.

## Running Airflow

Airflow uses a separate local environment and its official dependency
constraints. Follow [docs/airflow.md](docs/airflow.md) for installation and
startup commands. Point Airflow at this repository's `dags/` folder; do not
copy the DAG away from the `src/` package it imports.

## Continuous Integration

`.github/workflows/ci.yml` runs on pushes, pull requests, manual dispatches,
and every day at `00:00 UTC`. The nightly run uses the tiny committed fixture,
not the Git-ignored full MovieLens dataset. Reproduce its lightweight checks
locally:

```bash
python -m compileall -q app src dags tests
python -m unittest discover -s tests -v
SPARK_LOCAL_IP=127.0.0.1 \
SPARK_DRIVER_MEMORY=1g \
SPARK_EXECUTOR_MEMORY=1g \
SPARK_SHUFFLE_PARTITIONS=2 \
python -m src.pipeline run \
  --data-path tests/fixtures/ml-small \
  --output-path /tmp/movie-recommender-ci-output \
  --movie-title "Toy Story" \
  --top-n 3 \
  --processed-top-n 5
```

## Running with Docker

Make sure the MovieLens CSV files are available locally before building:

```text
data/raw/ml-latest/ratings.csv
data/raw/ml-latest/movies.csv
```

Build the image from the project root:

```bash
docker build -t movie-recommendation-pyspark .
```

Run the app:

```bash
docker run --env-file .env -p 8501:8501 movie-recommendation-pyspark
```

Then open:

```text
http://localhost:8501
```

The `.env` file is not copied into the image. Pass it with `--env-file .env` if
you want TMDB posters enabled.

## Project Structure

```text
.
├── app/
│   └── streamlit_app.py        # Streamlit user interface
├── dags/
│   └── movie_recommender_dag.py # Airflow orchestration
├── docs/
│   ├── airflow.md              # Local Airflow guide
│   └── data_engineering_tools_audit.md
├── assets/
│   ├── home_page.png           # Home page screenshot
│   └── recommendations.png     # Recommendations screenshot
├── data/
│   ├── raw/ml-latest/          # MovieLens CSV files, ignored by git
│   └── processed/              # Reserved for processed outputs
├── models/                     # Reserved for saved models
├── notebooks/
│   ├── 01_load_data.ipynb      # Data loading, EDA, and ALS experiments
│   └── 02_test_recommender.ipynb
├── src/
│   ├── __init__.py
│   ├── pipeline.py             # Reusable batch pipeline and CLI
│   └── recommend.py            # Recommendation and poster helper functions
├── tests/
│   ├── fixtures/ml-small/      # Tiny CI-only MovieLens sample
│   └── test_pipeline.py
├── .github/workflows/ci.yml    # Automated repository validation
├── .env.example                # Environment variable template
├── .gitignore
├── README.md
├── requirements-airflow.txt    # Airflow-only pinned dependency
└── requirements.txt
```

## Recommendation Logic

The production recommendation function is:

```python
recommend_similar_movies(movie_title, top_n=10, min_rating=4.5)
```

For a selected movie, it:

1. Finds matching MovieLens movie IDs.
2. Finds users who rated those movies at or above `min_rating`.
3. Finds other movies those users also rated at or above `min_rating`.
4. Counts how often each candidate movie appears.
5. Returns the top results with title, genres, and fan rating count.

## Screenshots

### Home Page

![Home Page](assets/home_page.png)

### Recommendations

![Recommendations](assets/recommendations.png)

## Future Improvements

- Add richer movie poster metadata and caching.
- Add ALS-based personalized recommendations.
- Save and version trained model artifacts.
- Add incremental data ingestion instead of full snapshot processing.
- Add persistent/shared artifact storage and pipeline monitoring.
- Add cloud deployment.
