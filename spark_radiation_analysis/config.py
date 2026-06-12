from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
BRONZE_CSV = BRONZE_DIR / "solar-radiation" / "2017_2019.csv"
SILVER_DIR = DATA_DIR / "silver"
SILVER_PREPARED_DIR = SILVER_DIR / "solar_radiation_prepared"
RESULTS_DIR = PROJECT_ROOT / "results"
TABLES_DIR = RESULTS_DIR / "tables"
PLOTS_DIR = RESULTS_DIR / "plots"

REQUIRED_DIRS = [
    BRONZE_DIR,
    SILVER_DIR,
    SILVER_PREPARED_DIR,
    TABLES_DIR,
    PLOTS_DIR,
]


def ensure_directories() -> None:
    for path in REQUIRED_DIRS:
        path.mkdir(parents=True, exist_ok=True)
