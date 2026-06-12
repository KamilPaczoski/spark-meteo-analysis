from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from spark_radiation_analysis.config import load_paths
from spark_radiation_analysis.ingestion.solar_radiation import (
    prepare_solar,
    write_solar,
)
from spark_radiation_analysis.spark import create_spark


def parse_args() -> argparse.Namespace:
    paths = load_paths()
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", type=Path, default=paths.solar_csv)
    parser.add_argument("--output-dir", type=Path, default=paths.solar_silver_dir)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark(app_name="prepare-solar-radiation")

    try:
        df = prepare_solar(spark=spark, path=args.input_csv).cache()

        rows = df.count()
        time_range = df.agg(
            F.min("event_timestamp").alias("min_event_timestamp"),
            F.max("event_timestamp").alias("max_event_timestamp"),
        ).collect()[0]

        write_solar(df=df, path=args.output_dir)

        print(f"Prepared rows: {rows}")
        print(
            f"Time range: {time_range['min_event_timestamp']} -> {time_range['max_event_timestamp']}"
        )
        print(f"Prepared dataset saved to: {args.output_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()