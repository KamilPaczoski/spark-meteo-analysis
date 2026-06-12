from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

STAT_COLS = [
    "GHI",
    "DNI",
    "DHI",
    "Temperature",
    "Relative Humidity",
    "Wind Speed",
    "Pressure",
]


def read_silver(spark: SparkSession, path: str | Path) -> DataFrame:
    return spark.read.parquet(str(path))


def stat_table(df: DataFrame) -> DataFrame:
    out = None
    for c in STAT_COLS:
        row = (
            df.agg(
                F.count(F.col(c)).alias("n"),
                F.avg(F.col(c)).alias("avg"),
                F.stddev(F.col(c)).alias("std"),
                F.min(F.col(c)).alias("min"),
                F.max(F.col(c)).alias("max"),
            )
            .withColumn("var", F.lit(c))
            .select("var", "n", "avg", "std", "min", "max")
        )
        out = row if out is None else out.unionByName(row)
    return out.orderBy("var")


def rel_table(df: DataFrame) -> DataFrame:
    return df.select(
        F.corr("GHI", "Temperature").alias("corr_ghi_temp"),
        F.corr("GHI", "Relative Humidity").alias("corr_ghi_rh"),
        F.corr("GHI", "Wind Speed").alias("corr_ghi_wind"),
    )


def hour_table(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("Hour")
        .agg(
            F.count("*").alias("n"),
            F.avg("GHI").alias("avg_ghi"),
            F.avg("Temperature").alias("avg_temp"),
            F.avg("Relative Humidity").alias("avg_rh"),
            F.avg("Wind Speed").alias("avg_wind"),
        )
        .orderBy("Hour")
    )


def scenario1_quality(df: DataFrame) -> DataFrame:
    q = df.agg(
        F.count("*").alias("rows_all"),
        F.count(
            F.when(
                F.col("GHI").isNotNull()
                & F.col("Temperature").isNotNull()
                & F.col("Relative Humidity").isNotNull()
                & F.col("Wind Speed").isNotNull(),
                1,
            )
        ).alias("rows_complete"),
        F.count(
            F.when(
                (F.col("GHI") >= 0) & (F.col("DNI") >= 0) & (F.col("DHI") >= 0),
                1,
            )
        ).alias("rows_non_negative"),
    )

    values = q.select(
        F.when(
            F.col("rows_all") == 0,
            F.lit(None),
        )
        .otherwise(F.col("rows_complete") / F.col("rows_all"))
        .alias("completeness_ratio"),
        F.when(
            F.col("rows_all") == 0,
            F.lit(None),
        )
        .otherwise(F.col("rows_non_negative") / F.col("rows_all"))
        .alias("non_negative_radiation_ratio"),
    )

    return values.selectExpr(
        "stack(2, "
        "'completeness_ratio', completeness_ratio, "
        "'non_negative_radiation_ratio', non_negative_radiation_ratio"
        ") as (metric, value)"
    )


def scenario2_quality(rel: DataFrame, hourly: DataFrame) -> DataFrame:
    peak = hourly.agg((F.max("avg_ghi") / F.avg("avg_ghi")).alias("hourly_peak_to_mean_ghi"))

    values = rel.crossJoin(peak).select(
        (
            (
                F.abs(F.col("corr_ghi_temp"))
                + F.abs(F.col("corr_ghi_rh"))
                + F.abs(F.col("corr_ghi_wind"))
            )
            / F.lit(3.0)
        ).alias("mean_abs_corr_ghi_weather"),
        F.col("hourly_peak_to_mean_ghi"),
    )

    return values.selectExpr(
        "stack(2, "
        "'mean_abs_corr_ghi_weather', mean_abs_corr_ghi_weather, "
        "'hourly_peak_to_mean_ghi', hourly_peak_to_mean_ghi"
        ") as (metric, value)"
    )


def save_table(df: DataFrame, path: str | Path) -> None:
    p = Path(path)
    df.write.mode("overwrite").parquet(str(p / "parquet"))
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(p / "csv"))
