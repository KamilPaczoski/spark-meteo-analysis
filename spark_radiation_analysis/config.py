from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    project_root: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    solar_csv: Path
    solar_silver_dir: Path
    solar_gold_dir: Path


def load_paths() -> DataPaths:
    default_root = Path(__file__).resolve().parents[1]
    project_root = Path(os.getenv("SPARK_METEO_PROJECT_ROOT", default_root)).resolve()

    bronze_dir = Path(os.getenv("SPARK_METEO_BRONZE_DIR", project_root / "data" / "bronze")).resolve()
    silver_dir = Path(os.getenv("SPARK_METEO_SILVER_DIR", project_root / "data" / "silver")).resolve()
    gold_dir = Path(os.getenv("SPARK_METEO_GOLD_DIR", project_root / "data" / "gold")).resolve()

    solar_csv = Path(
        os.getenv(
            "SPARK_METEO_SOLAR_CSV",
            bronze_dir / "solar-radiation" / "2017_2019.csv",
        )
    ).resolve()
    solar_silver_dir = Path(
        os.getenv(
            "SPARK_METEO_SOLAR_SILVER_DIR",
            silver_dir / "solar-radiation" / "prepared",
        )
    ).resolve()
    solar_gold_dir = Path(
        os.getenv(
            "SPARK_METEO_SOLAR_GOLD_DIR",
            gold_dir / "solar-radiation",
        )
    ).resolve()

    return DataPaths(
        project_root=project_root,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        solar_csv=solar_csv,
        solar_silver_dir=solar_silver_dir,
        solar_gold_dir=solar_gold_dir,
    )