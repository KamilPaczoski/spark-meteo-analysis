from __future__ import annotations

import csv
import math
import os
import tempfile
import unittest
from pathlib import Path

from spark_radiation_analysis.config import load_paths
from spark_radiation_analysis.jobs.solar_experiments import main as run_solar_experiments


class ExperimentsTest(unittest.TestCase):
    def _with_env(self, overrides: dict[str, str]) -> None:
        old = {k: os.environ.get(k) for k in overrides}
        for k, v in overrides.items():
            os.environ[k] = v
        try:
            run_solar_experiments()
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def _read_metrics(self, path: Path) -> dict[str, float]:
        with path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        return {r["metric"]: float(r["value"]) for r in rows}

    def test_solar_experiments_outputs_and_metrics(self) -> None:
        paths = load_paths()
        self.assertTrue(paths.solar_silver_dir.exists())

        with tempfile.TemporaryDirectory() as tmp:
            solar_gold_dir = Path(tmp) / "gold" / "solar-radiation"
            overrides = {
                "MPLBACKEND": "Agg",
                "SPARK_METEO_SOLAR_SILVER_DIR": str(paths.solar_silver_dir),
                "SPARK_METEO_SOLAR_GOLD_DIR": str(solar_gold_dir),
            }
            self._with_env(overrides)

            tables_root = solar_gold_dir / "tables"
            for name in [
                "profil_statystyczny",
                "korelacje",
                "profil_godzinowy",
                "jakosc_danych",
                "jakosc_analizy",
                "porownanie_trybow",
            ]:
                self.assertTrue((tables_root / name / "csv").exists())
                self.assertTrue((tables_root / name / "parquet").exists())

            report_dir = solar_gold_dir / "report"
            report_tables = report_dir / "tables"
            report = report_dir / "report.md"
            self.assertTrue(report.exists())

            for name in [
                "tabela1_profil_statystyczny.csv",
                "tabela2_korelacje.csv",
                "tabela3_profil_godzinowy.csv",
                "tabela4_jakosc_danych.csv",
                "tabela5_jakosc_analizy.csv",
                "tabela6_porownanie_trybow.csv",
            ]:
                self.assertTrue((report_tables / name).exists())

            for name in [
                "wykres1_ghi_godziny.png",
                "wykres2_korelacje.png",
                "wykres3_czas_przetwarzania.png",
                "wykres4_przepustowosc.png",
            ]:
                self.assertTrue((report_dir / "charts" / name).exists())

            text = report.read_text(encoding="utf-8")
            self.assertIn("porównanie trybów przetwarzania", text)
            self.assertIn("Miary jakości/wydajności", text)
            self.assertIn("Wykres 1", text)
            self.assertIn("Tabela 1", text)
            self.assertIn("Wsadowy", text)
            self.assertIn("Strumieniowy", text)
            self.assertIn("Tabela 6", text)

            q1 = self._read_metrics(report_tables / "tabela4_jakosc_danych.csv")
            q2 = self._read_metrics(report_tables / "tabela5_jakosc_analizy.csv")

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

            with (report_tables / "tabela6_porownanie_trybow.csv").open("r", encoding="utf-8", newline="") as f:
                cmp_rows = list(csv.DictReader(f))

            self.assertEqual(len(cmp_rows), 2)
            by_mode = {r["mode"]: r for r in cmp_rows}
            self.assertEqual(set(by_mode), {"wsadowy", "strumieniowy"})

            # Oba tryby przetwarzają ten sam wolumen wejścia i dają spójny wynik agregacji.
            in_b = int(float(by_mode["wsadowy"]["input_rows"]))
            in_s = int(float(by_mode["strumieniowy"]["input_rows"]))
            self.assertEqual(in_b, in_s)
            self.assertGreater(in_b, 0)

            agg_b = int(float(by_mode["wsadowy"]["agg_rows"]))
            agg_s = int(float(by_mode["strumieniowy"]["agg_rows"]))
            self.assertEqual(agg_b, agg_s)
            self.assertGreater(agg_b, 0)

            sec_b = float(by_mode["wsadowy"]["median_seconds"])
            sec_s = float(by_mode["strumieniowy"]["median_seconds"])
            self.assertGreater(sec_b, 0.0)
            self.assertGreater(sec_s, 0.0)

            thr_b = float(by_mode["wsadowy"]["rows_per_sec"])
            thr_s = float(by_mode["strumieniowy"]["rows_per_sec"])
            self.assertGreater(thr_b, 0.0)
            self.assertGreater(thr_s, 0.0)


if __name__ == "__main__":
    unittest.main()
