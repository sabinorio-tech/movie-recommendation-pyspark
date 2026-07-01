"""Download the MovieLens CSV files published with this repository."""

import hashlib
import os
import shutil
import tempfile
from pathlib import Path, PurePosixPath
from zipfile import BadZipFile, ZipFile

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "ml-latest"
DATASET_URL = (
    "https://github.com/sabinorio-tech/movie-recommendation-pyspark/"
    "releases/download/v1.0-data/movielens-data.zip"
)
DATASET_SHA256 = "fc6cb295ce8d32a95b325bdd43474444ee40076122765df9d79c10eca8144c5f"
REQUIRED_FILES = ("ratings.csv", "movies.csv")
DOWNLOAD_CHUNK_SIZE = 1024 * 1024


class DatasetDownloadError(RuntimeError):
    """Raised when the required MovieLens files cannot be prepared."""


def dataset_exists(data_path=DEFAULT_DATA_PATH):
    """Return whether both required, non-empty dataset files exist."""
    data_path = Path(data_path)
    return all(
        (data_path / filename).is_file()
        and (data_path / filename).stat().st_size > 0
        for filename in REQUIRED_FILES
    )


def _download_archive(archive_path):
    digest = hashlib.sha256()

    with requests.get(DATASET_URL, stream=True, timeout=(10, 300)) as response:
        response.raise_for_status()
        with Path(archive_path).open("wb") as archive_file:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    archive_file.write(chunk)
                    digest.update(chunk)

    if digest.hexdigest() != DATASET_SHA256:
        raise ValueError("the downloaded ZIP failed its SHA-256 integrity check")


def _extract_required_files(archive_path, staging_path):
    with ZipFile(archive_path) as archive:
        members = {}
        for member in archive.infolist():
            filename = PurePosixPath(member.filename).name
            if filename in REQUIRED_FILES and not member.is_dir():
                if filename in members:
                    raise ValueError(f"the ZIP contains duplicate {filename} files")
                members[filename] = member

        missing = sorted(set(REQUIRED_FILES).difference(members))
        if missing:
            raise ValueError(
                "the ZIP is missing required files: " + ", ".join(missing)
            )

        for filename in REQUIRED_FILES:
            destination = Path(staging_path) / filename
            with archive.open(members[filename]) as source:
                with destination.open("wb") as target:
                    shutil.copyfileobj(source, target, length=DOWNLOAD_CHUNK_SIZE)
            if destination.stat().st_size == 0:
                raise ValueError(f"the ZIP contains an empty {filename} file")


def ensure_movielens_data(data_path=DEFAULT_DATA_PATH):
    """Download and extract the required MovieLens files when they are absent."""
    data_path = Path(data_path)
    if dataset_exists(data_path):
        print("MovieLens dataset already exists.", flush=True)
        return data_path

    print("MovieLens dataset not found.", flush=True)
    print("Downloading dataset...", flush=True)

    archive_path = None
    staging_path = None

    try:
        data_path.mkdir(parents=True, exist_ok=True)
        archive_handle, archive_name = tempfile.mkstemp(
            prefix=".movielens-", suffix=".zip", dir=data_path
        )
        os.close(archive_handle)
        archive_path = Path(archive_name)
        staging_path = Path(
            tempfile.mkdtemp(prefix=".movielens-extract-", dir=data_path)
        )

        _download_archive(archive_path)
        print("Extracting...", flush=True)
        _extract_required_files(archive_path, staging_path)

        for filename in REQUIRED_FILES:
            os.replace(staging_path / filename, data_path / filename)

        print("Dataset ready.", flush=True)
        return data_path
    except (BadZipFile, OSError, requests.RequestException, ValueError) as error:
        message = (
            "The MovieLens dataset could not be downloaded or extracted. "
            f"Check network access and the GitHub Release asset. Details: {error}"
        )
        print(f"Dataset download failed: {error}", flush=True)
        raise DatasetDownloadError(message) from error
    finally:
        if archive_path is not None:
            try:
                archive_path.unlink(missing_ok=True)
            except OSError:
                pass
        if staging_path is not None:
            shutil.rmtree(staging_path, ignore_errors=True)


if __name__ == "__main__":
    ensure_movielens_data()
