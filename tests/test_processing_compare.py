from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from spark_radiation_analysis.analytics.processing_compare import compare_processing_modes
from spark_radiation_analysis.spark import create_spark


class ProcessingCompareTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("test-processing-compare")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_compare_modes_metrics_and_consistency(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "sample.csv"
            # Dwie różne godziny -> agregacja godzinowa musi dać 2 wiersze wyniku.
            csv_path.write_text(
                "Year,Month,Day,Hour,Minute,Temperature,Clearsky DHI,Clearsky DNI,Clearsky GHI,Dew Point,DHI,DNI,GHI,Relative Humidity,Solar Zenith Angle,Surface Albedo,Pressure,Wind Speed,Unnamed: 18\n"
                "2017,1,1,0,0,10,0,0,0,1,0,0,100,50,100,0.1,1000,2.0,\n"
                "2017,1,1,0,0,11,0,0,0,1,0,0,120,49,99,0.1,1000,2.1,\n"
                "2017,1,1,1,0,12,0,0,0,1,0,0,140,48,98,0.1,1000,2.2,\n",
                encoding="utf-8",
            )

            out = compare_processing_modes(self.spark, csv_path, Path(tmp) / "work", repeats=2)
            rows = {r["mode"]: r for r in out.collect()}

            self.assertEqual(set(rows), {"wsadowy", "strumieniowy"})

            for mode in ("wsadowy", "strumieniowy"):
                r = rows[mode]
                self.assertEqual(r["runs"], 2)
                self.assertEqual(r["input_rows"], 3)
                # Agregacja po godzinie: dwie godziny -> dwa wiersze wyniku w obu trybach.
                self.assertEqual(r["agg_rows"], 2)
                self.assertGreater(r["median_seconds"], 0.0)
                self.assertGreater(r["min_seconds"], 0.0)
                self.assertGreater(r["rows_per_sec"], 0.0)

            # Kontrola spójności: oba tryby liczą ten sam wynik analityczny.
            self.assertEqual(rows["wsadowy"]["agg_rows"], rows["strumieniowy"]["agg_rows"])


if __name__ == "__main__":
    unittest.main()
