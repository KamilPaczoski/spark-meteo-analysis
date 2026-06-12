from __future__ import annotations

import math
import unittest

from spark_radiation_analysis.config import load_paths
from spark_radiation_analysis.analytics.solar import (
    hour_table,
    read_silver,
    rel_table,
    scenario1_quality,
    scenario2_quality,
    stat_table,
)
from spark_radiation_analysis.spark import create_spark


class AnalyticsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("test-analytics")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_stat_table(self) -> None:
        df = self.spark.createDataFrame(
            [
                (0, 100.0, 200.0, 50.0, 10.0, 80.0, 1.0, 1000.0),
                (0, 200.0, 300.0, 60.0, 20.0, 70.0, 2.0, 1001.0),
                (1, 300.0, 400.0, 70.0, 30.0, 60.0, 3.0, 1002.0),
                (1, 400.0, 500.0, 80.0, 40.0, 50.0, 4.0, 1003.0),
            ],
            [
                "Hour",
                "GHI",
                "DNI",
                "DHI",
                "Temperature",
                "Relative Humidity",
                "Wind Speed",
                "Pressure",
            ],
        )

        out = stat_table(df)
        ghi = out.where("var = 'GHI'").collect()[0]

        self.assertEqual(ghi["n"], 4)
        self.assertEqual(ghi["min"], 100.0)
        self.assertEqual(ghi["max"], 400.0)
        self.assertAlmostEqual(ghi["avg"], 250.0, places=6)

    def test_rel_and_hour_table(self) -> None:
        df = self.spark.createDataFrame(
            [
                (0, 100.0, 200.0, 50.0, 10.0, 80.0, 1.0, 1000.0),
                (0, 200.0, 300.0, 60.0, 20.0, 70.0, 2.0, 1001.0),
                (1, 300.0, 400.0, 70.0, 30.0, 60.0, 3.0, 1002.0),
                (1, 400.0, 500.0, 80.0, 40.0, 50.0, 4.0, 1003.0),
            ],
            [
                "Hour",
                "GHI",
                "DNI",
                "DHI",
                "Temperature",
                "Relative Humidity",
                "Wind Speed",
                "Pressure",
            ],
        )

        rel = rel_table(df).collect()[0]
        self.assertAlmostEqual(rel["corr_ghi_temp"], 1.0, places=6)
        self.assertAlmostEqual(rel["corr_ghi_rh"], -1.0, places=6)
        self.assertAlmostEqual(rel["corr_ghi_wind"], 1.0, places=6)

        hour = hour_table(df).collect()
        self.assertEqual(len(hour), 2)
        self.assertEqual(hour[0]["Hour"], 0)
        self.assertAlmostEqual(hour[0]["avg_ghi"], 150.0, places=6)
        self.assertEqual(hour[1]["Hour"], 1)
        self.assertAlmostEqual(hour[1]["avg_ghi"], 350.0, places=6)

    def test_quality_tables(self) -> None:
        df = self.spark.createDataFrame(
            [
                (0, 100.0, 200.0, 50.0, 10.0, 80.0, 1.0, 1000.0),
                (0, 200.0, 300.0, 60.0, 20.0, 70.0, 2.0, 1001.0),
                (1, -1.0, 400.0, 70.0, 30.0, 60.0, 3.0, 1002.0),
                (1, 400.0, 500.0, 80.0, None, 50.0, 4.0, 1003.0),
            ],
            [
                "Hour",
                "GHI",
                "DNI",
                "DHI",
                "Temperature",
                "Relative Humidity",
                "Wind Speed",
                "Pressure",
            ],
        )

        q1 = {r["metric"]: r["value"] for r in scenario1_quality(df).collect()}
        self.assertAlmostEqual(q1["completeness_ratio"], 0.75, places=6)
        self.assertAlmostEqual(q1["non_negative_radiation_ratio"], 0.75, places=6)

        rel = rel_table(df.fillna({"Temperature": 25.0}))
        hourly = hour_table(df.fillna({"Temperature": 25.0}))
        q2 = {r["metric"]: r["value"] for r in scenario2_quality(rel, hourly).collect()}

        self.assertGreaterEqual(q2["mean_abs_corr_ghi_weather"], 0.0)
        self.assertGreater(q2["hourly_peak_to_mean_ghi"], 0.0)

    def test_quality_metrics_on_project_silver(self) -> None:
        paths = load_paths()
        self.assertTrue(paths.solar_silver_dir.exists())

        df = read_silver(self.spark, paths.solar_silver_dir)
        rel = rel_table(df)
        hourly = hour_table(df)

        q1 = {r["metric"]: r["value"] for r in scenario1_quality(df).collect()}
        q2 = {r["metric"]: r["value"] for r in scenario2_quality(rel, hourly).collect()}

        self.assertEqual(set(q1), {"completeness_ratio", "non_negative_radiation_ratio"})
        self.assertEqual(set(q2), {"mean_abs_corr_ghi_weather", "hourly_peak_to_mean_ghi"})

        self.assertFalse(math.isnan(q1["completeness_ratio"]))
        self.assertFalse(math.isnan(q1["non_negative_radiation_ratio"]))
        self.assertFalse(math.isnan(q2["mean_abs_corr_ghi_weather"]))
        self.assertFalse(math.isnan(q2["hourly_peak_to_mean_ghi"]))

        self.assertGreaterEqual(q1["completeness_ratio"], 0.0)
        self.assertLessEqual(q1["completeness_ratio"], 1.0)
        self.assertGreaterEqual(q1["non_negative_radiation_ratio"], 0.0)
        self.assertLessEqual(q1["non_negative_radiation_ratio"], 1.0)
        self.assertGreaterEqual(q2["mean_abs_corr_ghi_weather"], 0.0)
        self.assertLessEqual(q2["mean_abs_corr_ghi_weather"], 1.0)
        self.assertGreaterEqual(q2["hourly_peak_to_mean_ghi"], 1.0)


if __name__ == "__main__":
    unittest.main()
