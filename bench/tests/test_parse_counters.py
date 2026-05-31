import csv
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parse_counters import counter_rows, parse_history_file, write_counters, write_wall_clock


HISTORY_DIR = Path(__file__).parent / "fixtures" / "history"
HISTORY_FILES = [
    HISTORY_DIR / "stage0a_0089.jhist",
    HISTORY_DIR / "stage0b_0090.jhist",
    HISTORY_DIR / "stage1_j1a_0091.jhist",
    HISTORY_DIR / "stage1_j1b_0092.jhist",
    HISTORY_DIR / "stage2_0093.jhist",
    HISTORY_DIR / "stage3_sort_0094.jhist",
    HISTORY_DIR / "stage3_topn_0095.jhist",
]


class ParseCountersTest(unittest.TestCase):
    def load_jobs(self):
        jobs = []
        for path in HISTORY_FILES:
            jobs.extend(parse_history_file(path))
        return jobs

    def test_parses_real_pipeline_jhist_files(self):
        jobs = self.load_jobs()
        names = [job.job_name for job in jobs]

        self.assertEqual(7, len(jobs))
        self.assertIn("Stage0aFreqJob [weichenyin-cbe21c8-20260530221139]", names)
        self.assertIn("Stage0bFilterJob [weichenyin-cbe21c8-20260530221139]", names)
        self.assertIn("Stage1Job j1a [weichenyin-cbe21c8-20260530221139]", names)
        self.assertIn("Stage1Job j1b [weichenyin-cbe21c8-20260530221139]", names)
        self.assertIn("Stage2Job [weichenyin-cbe21c8-20260530221139]", names)
        self.assertIn("Stage3SortJob", names)
        self.assertIn("Stage3TopNJob", names)
        self.assertEqual(["stage0", "stage0", "stage1", "stage1", "stage2", "stage3", "stage3"],
                         [job.inferred_stage() for job in jobs])

    def test_parses_documented_counters_from_real_jhist(self):
        rows = counter_rows(self.load_jobs())
        by_key = {
            (row.job_id, row.counter_group, row.counter_name): row.value
            for row in rows
        }

        self.assertEqual(9279659, by_key[("job_1515238638289_0089", "STAGE0", "RAW_RECORDS")])
        self.assertEqual(1307973, by_key[("job_1515238638289_0089", "STAGE0", "SINGLETON_VIDS")])
        self.assertEqual(7971686, by_key[("job_1515238638289_0090", "STAGE0", "KEPT_RECORDS")])
        self.assertEqual(966776678, by_key[("job_1515238638289_0091", "STAGE1", "PAIRS_EMITTED")])
        self.assertEqual(311300605, by_key[("job_1515238638289_0092", "STAGE1", "PAIRS_EMITTED")])
        self.assertEqual(1278077283, by_key[("job_1515238638289_0093", "STAGE2", "PAIRS_INPUT")])
        self.assertEqual(23071570, by_key[("job_1515238638289_0093", "STAGE2", "PAIRS_OUTPUT")])
        self.assertEqual(10000, by_key[("job_1515238638289_0095", "STAGE3", "TOPN_EMITTED")])

    def test_writes_stable_csv_schemas_from_real_jhist(self):
        jobs = self.load_jobs()
        rows = counter_rows(jobs)

        with tempfile.TemporaryDirectory() as tmp:
            counters_path = Path(tmp) / "counters.csv"
            wall_path = Path(tmp) / "wall_clock.csv"
            write_counters(str(counters_path), rows)
            write_wall_clock(str(wall_path), jobs)

            with counters_path.open(newline="", encoding="utf-8") as src:
                reader = csv.reader(src)
                self.assertEqual(
                    ["job_id", "job_name", "stage", "counter_group", "counter_name", "value"],
                    next(reader),
                )

            with wall_path.open(newline="", encoding="utf-8") as src:
                reader = csv.DictReader(src)
                wall_rows = {row["job_id"]: row for row in reader}
                self.assertEqual("71622", wall_rows["job_1515238638289_0089"]["wall_clock_ms"])
                self.assertEqual("1801042", wall_rows["job_1515238638289_0093"]["wall_clock_ms"])


if __name__ == "__main__":
    unittest.main()
