from __future__ import annotations

from time import perf_counter

import pandas as pd
from pyspark import StorageLevel

from spark_radiation_analysis.processing import (
    daytime_monthly_aggregation,
    load_and_prepare_data,
    load_prepared_data,
)


# Wykonuje jeden scenariusz eksperymentu baseline / optimized dla bronze CSV.
def _run_single_experiment(spark, csv_path, scenario: str, use_schema: bool, optimized: bool) -> tuple[pd.DataFrame, dict]:
    total_start = perf_counter()

    load_start = perf_counter()
    df = load_and_prepare_data(spark, csv_path, use_schema=use_schema)
    if optimized:
        # Optymalizacja: wybór potrzebnych kolumn, repartition i cache.
        df = df.select(
            "timestamp",
            "Month",
            "Temperature",
            "DHI",
            "DNI",
            "GHI",
            "Relative_Humidity",
            "Solar_Zenith_Angle",
            "Wind_Speed",
        ).repartition(8, "Month")
        df.persist(StorageLevel.MEMORY_AND_DISK)
    rows_processed = df.count()
    load_time = perf_counter() - load_start

    processing_start = perf_counter()
    monthly_df = daytime_monthly_aggregation(df)
    monthly_pdf = monthly_df.toPandas()
    processing_time = perf_counter() - processing_start
    total_time = perf_counter() - total_start

    if optimized:
        df.unpersist()

    metrics = {
        "scenario": scenario,
        "load_time_s": round(load_time, 4),
        "processing_time_s": round(processing_time, 4),
        "total_pipeline_time_s": round(total_time, 4),
        "rows_processed": int(rows_processed),
        "rows_per_second": round(rows_processed / total_time, 2) if total_time else 0.0,
        "partitions": monthly_df.rdd.getNumPartitions(),
    }
    return monthly_pdf, metrics


# Porównanie baseline i optimized na surowej warstwie bronze.
def run_all_experiments(spark, csv_path):
    baseline_table, baseline_metrics = _run_single_experiment(
        spark=spark,
        csv_path=csv_path,
        scenario="baseline",
        use_schema=False,
        optimized=False,
    )
    optimized_table, optimized_metrics = _run_single_experiment(
        spark=spark,
        csv_path=csv_path,
        scenario="optimized",
        use_schema=True,
        optimized=True,
    )

    speedup = (
        baseline_metrics["total_pipeline_time_s"] / optimized_metrics["total_pipeline_time_s"]
        if optimized_metrics["total_pipeline_time_s"]
        else 0.0
    )
    optimized_metrics["speedup_vs_baseline"] = round(speedup, 4)
    baseline_metrics["speedup_vs_baseline"] = 1.0

    metrics_df = pd.DataFrame([baseline_metrics, optimized_metrics])
    return baseline_table, optimized_table, metrics_df


# Wspólny workload do uczciwego porównania bronze i silver.
def _timed_dataset_pipeline(df) -> tuple[pd.DataFrame, dict]:
    start = perf_counter()
    rows = df.count()
    load_and_count_time = perf_counter() - start

    processing_start = perf_counter()
    monthly_pdf = daytime_monthly_aggregation(df).toPandas()
    processing_time = perf_counter() - processing_start
    total_time = load_and_count_time + processing_time

    metrics = {
        "rows_processed": int(rows),
        "load_and_count_time_s": round(load_and_count_time, 4),
        "processing_time_s": round(processing_time, 4),
        "total_time_s": round(total_time, 4),
        "rows_per_second": round(rows / total_time, 2) if total_time else 0.0,
    }
    return monthly_pdf, metrics


# Porównuje ten sam workflow na bronze CSV i silver Parquet.
def compare_bronze_vs_silver(spark, bronze_csv_path, silver_path):
    bronze_df = load_and_prepare_data(spark, bronze_csv_path, use_schema=True)
    bronze_table, bronze_metrics = _timed_dataset_pipeline(bronze_df)
    bronze_metrics["layer"] = "bronze"

    silver_df = load_prepared_data(spark, silver_path)
    silver_table, silver_metrics = _timed_dataset_pipeline(silver_df)
    silver_metrics["layer"] = "silver"

    bronze_total = bronze_metrics["total_time_s"]
    silver_total = silver_metrics["total_time_s"]
    silver_metrics["speedup_vs_bronze"] = round(bronze_total / silver_total, 4) if silver_total else 0.0
    bronze_metrics["speedup_vs_bronze"] = 1.0

    metrics_df = pd.DataFrame([bronze_metrics, silver_metrics])
    return bronze_table, silver_table, metrics_df
