import csv
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parse_counters import reducer_attempt_rows_from_history_file, write_reducer_attempts
from reducer_skew import load_attempts, metric_summary, write_detail, write_summary


STAGE2_HISTORY = Path(__file__).parent / "fixtures" / "history" / "stage2_0093.jhist"


class ReducerSkewTest(unittest.TestCase):
    def write_real_attempt_fixture(self, tmp: str) -> Path:
        attempts_path = Path(tmp) / "reducer_attempts.csv"
        rows = reducer_attempt_rows_from_history_file(STAGE2_HISTORY)
        write_reducer_attempts(str(attempts_path), rows)
        return attempts_path

    def test_derives_reducer_attempts_from_real_jhist(self):
        rows = reducer_attempt_rows_from_history_file(STAGE2_HISTORY)

        self.assertEqual(8, len(rows))
        self.assertEqual({str(i) for i in range(8)}, {row.reducer_id for row in rows})
        self.assertTrue(all(row.status == "SUCCEEDED" for row in rows))
        self.assertTrue(all(row.records >= 0 for row in rows))
        self.assertTrue(all(row.shuffle_bytes > 0 for row in rows))
        self.assertTrue(all(row.wall_ms > 0 for row in rows))

    def test_loads_derived_attempt_csv_and_filters_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            attempts_path = self.write_real_attempt_fixture(tmp)
            attempts = load_attempts(str(attempts_path))

        self.assertEqual(8, len(attempts))
        self.assertEqual({str(i) for i in range(8)}, {attempt.reducer_id for attempt in attempts})

    def test_metric_summary_from_real_reducer_attempts(self):
        with tempfile.TemporaryDirectory() as tmp:
            attempts_path = self.write_real_attempt_fixture(tmp)
            attempts = load_attempts(str(attempts_path))

        summary = metric_summary([attempt.shuffle_bytes for attempt in attempts])
        self.assertEqual(8, summary["count"])
        self.assertGreater(summary["mean"], 0)
        self.assertGreater(summary["max"], 0)
        self.assertGreaterEqual(summary["max_mean_ratio"], 1.0)

    def test_writes_detail_and_summary_from_real_attempts(self):
        with tempfile.TemporaryDirectory() as tmp:
            attempts_path = self.write_real_attempt_fixture(tmp)
            attempts = load_attempts(str(attempts_path))
            detail = Path(tmp) / "skew.csv"
            summary = Path(tmp) / "skew_summary.csv"

            write_detail(str(detail), attempts)
            write_summary(str(summary), attempts)

            with detail.open(newline="", encoding="utf-8") as src:
                detail_rows = list(csv.DictReader(src))
                self.assertEqual(8, len(detail_rows))
                self.assertEqual(
                    ["reducer_id", "records", "shuffle_bytes", "wall_ms", "status", "attempt_id"],
                    list(detail_rows[0].keys()),
                )

            with summary.open(newline="", encoding="utf-8") as src:
                summary_rows = {row["metric"]: row for row in csv.DictReader(src)}
                self.assertIn("records", summary_rows)
                self.assertIn("shuffle_bytes", summary_rows)
                self.assertIn("wall_ms", summary_rows)


if __name__ == "__main__":
    unittest.main()
