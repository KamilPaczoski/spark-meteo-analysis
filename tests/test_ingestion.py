from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from spark_radiation_analysis.config import load_paths
from spark_radiation_analysis.ingestion.solar_radiation import (
    SOLAR_COLUMNS,
    clean_solar,
    prepare_solar,
    solar_schema,
)
from spark_radiation_analysis.spark import create_spark


class IngestionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark("test-ingestion")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_schema_cols(self) -> None:
        self.assertEqual(solar_schema().fieldNames(), SOLAR_COLUMNS)

    def test_clean_solar_cast_and_filter(self) -> None:
        rows = [
            (
                "2017",
                "1",
                "1",
                "0",
                "0",
                "10.0",
                "0",
                "0",
                "0",
                "1",
                "0",
                "0",
                "100",
                "50",
                "100",
                "0.1",
                "1000",
                "2.0",
                "",
            ),
            (
                "",
                "1",
                "1",
                "0",
                "0",
                "10.0",
                "0",
                "0",
                "0",
                "1",
                "0",
                "0",
                "100",
                "50",
                "100",
                "0.1",
                "1000",
                "2.0",
                "",
            ),
        ]
        raw = self.spark.createDataFrame(rows, solar_schema())

        out = clean_solar(raw)
        self.assertEqual(out.count(), 1)
        self.assertNotIn("Unnamed: 18", out.columns)

        row = out.collect()[0]
        self.assertEqual(row["Year"], 2017)
        self.assertEqual(row["GHI"], 100.0)
        self.assertIsNotNone(row["event_timestamp"])

    def test_prepare_solar_from_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "sample.csv"
            csv_path.write_text(
                "Year,Month,Day,Hour,Minute,Temperature,Clearsky DHI,Clearsky DNI,Clearsky GHI,Dew Point,DHI,DNI,GHI,Relative Humidity,Solar Zenith Angle,Surface Albedo,Pressure,Wind Speed,Unnamed: 18\n"
                "2017,1,1,0,0,10,0,0,0,1,0,0,100,50,100,0.1,1000,2.0,\n",
                encoding="utf-8",
            )

            out = prepare_solar(self.spark, csv_path)
            self.assertEqual(out.count(), 1)
            self.assertIn("event_timestamp", out.columns)

    def test_prepare_solar_from_project_bronze(self) -> None:
        paths = load_paths()
        self.assertTrue(paths.solar_csv.exists())

        out = prepare_solar(self.spark, paths.solar_csv)
        self.assertGreater(out.count(), 100000)

        ts = out.selectExpr(
            "min(event_timestamp) as min_ts",
            "max(event_timestamp) as max_ts",
        ).collect()[0]
        self.assertIsNotNone(ts["min_ts"])
        self.assertIsNotNone(ts["max_ts"])


if __name__ == "__main__":
    unittest.main()
