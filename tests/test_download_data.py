import hashlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from zipfile import ZipFile

import requests

from src.download_data import (
    DatasetDownloadError,
    dataset_exists,
    ensure_movielens_data,
)


def make_dataset_zip(files=None):
    files = files or {
        "ratings.csv": "userId,movieId,rating,timestamp\n1,1,5.0,1\n",
        "movies.csv": "movieId,title,genres\n1,Toy Story,Animation\n",
    }
    archive_bytes = io.BytesIO()
    with ZipFile(archive_bytes, "w") as archive:
        for filename, contents in files.items():
            archive.writestr(filename, contents)
    return archive_bytes.getvalue()


def mock_response(contents):
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.iter_content.return_value = [contents]
    return response


class DownloadDataTests(unittest.TestCase):
    def test_existing_dataset_skips_download(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            data_path = Path(temp_directory)
            for filename in ("ratings.csv", "movies.csv"):
                (data_path / filename).write_text("header\n", encoding="utf-8")

            with patch("src.download_data.requests.get") as request:
                result = ensure_movielens_data(data_path)

            self.assertEqual(result, data_path)
            self.assertTrue(dataset_exists(data_path))
            request.assert_not_called()

    def test_missing_dataset_is_downloaded_and_extracted(self):
        archive = make_dataset_zip()
        with tempfile.TemporaryDirectory() as temp_directory:
            data_path = Path(temp_directory) / "nested" / "data"
            data_path.mkdir(parents=True)
            (data_path / "ratings.csv").write_text("stale\n", encoding="utf-8")
            with (
                patch(
                    "src.download_data.DATASET_SHA256",
                    hashlib.sha256(archive).hexdigest(),
                ),
                patch(
                    "src.download_data.requests.get",
                    return_value=mock_response(archive),
                ),
            ):
                ensure_movielens_data(data_path)

            self.assertTrue(dataset_exists(data_path))
            self.assertEqual(list(data_path.glob("*.zip")), [])
            self.assertIn("userId,movieId", (data_path / "ratings.csv").read_text())

    def test_network_failure_is_reported(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            data_path = Path(temp_directory)
            with (
                patch(
                    "src.download_data.requests.get",
                    side_effect=requests.ConnectionError("network unavailable"),
                ),
                self.assertRaisesRegex(DatasetDownloadError, "network access"),
            ):
                ensure_movielens_data(data_path)

            self.assertEqual(list(data_path.glob("*.zip")), [])

    def test_download_failure_is_reported_and_temporary_zip_is_removed(self):
        archive = make_dataset_zip({"ratings.csv": "ratings\n"})
        with tempfile.TemporaryDirectory() as temp_directory:
            data_path = Path(temp_directory)
            with (
                patch(
                    "src.download_data.DATASET_SHA256",
                    hashlib.sha256(archive).hexdigest(),
                ),
                patch(
                    "src.download_data.requests.get",
                    return_value=mock_response(archive),
                ),
                self.assertRaisesRegex(DatasetDownloadError, "could not be downloaded"),
            ):
                ensure_movielens_data(data_path)

            self.assertEqual(list(data_path.glob("*.zip")), [])
            self.assertFalse(dataset_exists(data_path))


if __name__ == "__main__":
    unittest.main()
