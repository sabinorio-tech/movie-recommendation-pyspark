import tempfile
import unittest
from pathlib import Path

from src.pipeline import check_raw_data_exists, validate_input_schema


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ml-small"


class PipelineValidationTests(unittest.TestCase):
    def test_fixture_files_and_schemas_are_valid(self):
        file_result = check_raw_data_exists(FIXTURE_PATH)
        schema_result = validate_input_schema(FIXTURE_PATH)

        self.assertEqual(set(file_result["files"]), {"ratings.csv", "movies.csv"})
        self.assertEqual(schema_result["status"], "valid")

    def test_missing_input_files_fail_fast(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            with self.assertRaises(FileNotFoundError):
                check_raw_data_exists(temp_directory)

    def test_missing_required_column_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            data_path = Path(temp_directory)
            (data_path / "ratings.csv").write_text(
                "userId,movieId,rating\n1,1,5.0\n",
                encoding="utf-8",
            )
            (data_path / "movies.csv").write_text(
                "movieId,title,genres\n1,Toy Story (1995),Animation\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "timestamp"):
                validate_input_schema(data_path)


if __name__ == "__main__":
    unittest.main()
