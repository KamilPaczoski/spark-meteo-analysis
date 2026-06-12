from __future__ import annotations

from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from spark_radiation_analysis.schema import NORMALIZED_COLUMNS, RAW_SOLAR_SCHEMA


# Odczyt bronze CSV
def load_csv_with_schema(spark, csv_path: str | Path, schema: StructType = RAW_SOLAR_SCHEMA) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("encoding", "utf-8")
        .schema(schema)
        .csv(str(csv_path))
    )


# Odczyt bazowy używany do porównania z inferSchema.
def load_csv_baseline(spark, csv_path: str | Path) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("encoding", "utf-8")
        .csv(str(csv_path))
    )


# Porządkuje nazwy kolumn i usuwa pustą końcową kolumnę z CSV.
def normalize_columns(df: DataFrame) -> DataFrame:
    cleaned = df
    first_col = cleaned.columns[0]
    if first_col.startswith("\ufeff"):
        cleaned = cleaned.withColumnRenamed(first_col, first_col.replace("\ufeff", ""))
    for old_name, new_name in NORMALIZED_COLUMNS.items():
        if old_name in cleaned.columns:
            cleaned = cleaned.withColumnRenamed(old_name, new_name)
    if "Unnamed: 18" in cleaned.columns:
        cleaned = cleaned.drop("Unnamed: 18")
    return cleaned


# Dodaje timestamp i usuwa podstawowe błędne rekordy radiacyjne.
def add_timestamp_and_clean(df: DataFrame) -> DataFrame:
    prepared = normalize_columns(df).withColumn(
        "timestamp",
        F.to_timestamp(
            F.format_string(
                "%04d-%02d-%02d %02d:%02d:00",
                F.col("Year"),
                F.col("Month"),
                F.col("Day"),
                F.col("Hour"),
                F.col("Minute"),
            )
        ),
    )
    return prepared.filter(F.col("timestamp").isNotNull()).filter(
        (F.col("GHI") >= 0) & (F.col("DNI") >= 0) & (F.col("DHI") >= 0)
    )


# Główny helper dla bronze -> prepared DataFrame.
def load_and_prepare_data(spark, csv_path: str | Path, use_schema: bool = True) -> DataFrame:
    loader = load_csv_with_schema if use_schema else load_csv_baseline
    return add_timestamp_and_clean(loader(spark, csv_path))


# Odczyt gotowej warstwy silver zapisanej jako Parquet.
def load_prepared_data(spark, silver_path: str | Path) -> DataFrame:
    return spark.read.parquet(str(silver_path))


# Prosta agregacja miesięczna dla części dziennej danych.
def daytime_monthly_aggregation(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("GHI") > 0)
        .groupBy("Month")
        .agg(
            F.avg("GHI").alias("avg_ghi"),
            F.avg("DNI").alias("avg_dni"),
            F.avg("DHI").alias("avg_dhi"),
            F.avg("Temperature").alias("avg_temperature"),
        )
        .orderBy("Month")
    )


# Case 1: kiedy średnie GHI jest najwyższe i w jakich warunkach.
def build_ghi_research_outputs(df: DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    monthly = (
        df.filter(F.col("GHI") > 0)
        .groupBy("Month")
        .agg(
            F.avg("GHI").alias("avg_ghi"),
            F.avg("Temperature").alias("avg_temperature"),
            F.avg("Relative_Humidity").alias("avg_relative_humidity"),
            F.avg("Wind_Speed").alias("avg_wind_speed"),
        )
        .orderBy(F.desc("avg_ghi"))
        .toPandas()
    )

    humidity_band = (
        df.filter(F.col("GHI") > 0)
        .withColumn(
            "humidity_band",
            F.when(F.col("Relative_Humidity") < 30, "<30")
            .when(F.col("Relative_Humidity") < 50, "30-50")
            .when(F.col("Relative_Humidity") < 70, "50-70")
            .otherwise(">=70"),
        )
        .groupBy("humidity_band")
        .agg(
            F.avg("GHI").alias("avg_ghi"),
            F.avg("Temperature").alias("avg_temperature"),
            F.count("*").alias("rows"),
        )
        .orderBy("humidity_band")
        .toPandas()
    )

    corr = df.select(
        F.corr("GHI", "Temperature").alias("corr_ghi_temperature"),
        F.corr("GHI", "Relative_Humidity").alias("corr_ghi_relative_humidity"),
        F.corr("GHI", "Solar_Zenith_Angle").alias("corr_ghi_solar_zenith_angle"),
        F.corr("GHI", "Wind_Speed").alias("corr_ghi_wind_speed"),
    ).toPandas()

    return monthly, humidity_band, corr


# Case 2: które zmienne są najmocniej powiązane z Relative Humidity.
def build_relative_humidity_correlation_table(df: DataFrame) -> pd.DataFrame:
    corr_row = df.select(
        F.corr("Relative_Humidity", "Temperature").alias("Temperature"),
        F.corr("Relative_Humidity", "GHI").alias("GHI"),
        F.corr("Relative_Humidity", "DNI").alias("DNI"),
        F.corr("Relative_Humidity", "DHI").alias("DHI"),
        F.corr("Relative_Humidity", "Solar_Zenith_Angle").alias("Solar_Zenith_Angle"),
        F.corr("Relative_Humidity", "Wind_Speed").alias("Wind_Speed"),
        F.corr("Relative_Humidity", "Pressure").alias("Pressure"),
        F.corr("Relative_Humidity", "Surface_Albedo").alias("Surface_Albedo"),
    ).toPandas()

    long_df = corr_row.T.reset_index()
    long_df.columns = ["feature", "correlation_with_relative_humidity"]
    long_df["abs_correlation"] = long_df["correlation_with_relative_humidity"].abs()
    long_df = long_df.sort_values("abs_correlation", ascending=False).reset_index(drop=True)
    return long_df


# Zapis małych tabel wynikowych do CSV.
def save_pandas_table(df: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path


# Zapis prepared DataFrame do silver jako Parquet.
def save_prepared_data(df: DataFrame, path: str | Path, mode: str = "overwrite") -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode(mode).parquet(str(output_path))
    return output_path
