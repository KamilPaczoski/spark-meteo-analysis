from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

SOLAR_COLUMNS = [
    "Year",
    "Month",
    "Day",
    "Hour",
    "Minute",
    "Temperature",
    "Clearsky DHI",
    "Clearsky DNI",
    "Clearsky GHI",
    "Dew Point",
    "DHI",
    "DNI",
    "GHI",
    "Relative Humidity",
    "Solar Zenith Angle",
    "Surface Albedo",
    "Pressure",
    "Wind Speed",
    "Unnamed: 18",
]

NUMERIC_COLUMNS = [
    "Temperature",
    "Clearsky DHI",
    "Clearsky DNI",
    "Clearsky GHI",
    "Dew Point",
    "DHI",
    "DNI",
    "GHI",
    "Relative Humidity",
    "Solar Zenith Angle",
    "Surface Albedo",
    "Pressure",
    "Wind Speed",
]


def solar_schema() -> StructType:
    return StructType([StructField(col, StringType(), True) for col in SOLAR_COLUMNS])


def read_solar_csv(spark: SparkSession, path: str | Path) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .option("enforceSchema", "true")
        .schema(solar_schema())
        .csv(str(path))
    )


def clean_solar(df: DataFrame) -> DataFrame:
    clean_df = df
    for col in df.columns:
        clean_df = clean_df.withColumn(
            col,
            F.when(
                F.col(col).isNull() | (F.trim(F.col(col)) == ""),
                F.lit(None),
            ).otherwise(F.trim(F.col(col))),
        )

    typed_df = (
        clean_df.withColumn("Year", F.col("Year").cast("int"))
        .withColumn("Month", F.col("Month").cast("int"))
        .withColumn("Day", F.col("Day").cast("int"))
        .withColumn("Hour", F.col("Hour").cast("int"))
        .withColumn("Minute", F.col("Minute").cast("int"))
    )

    for col in NUMERIC_COLUMNS:
        typed_df = typed_df.withColumn(col, F.col(col).cast("double"))

    typed_df = typed_df.withColumn(
        "event_timestamp",
        F.make_timestamp(
            F.col("Year"),
            F.col("Month"),
            F.col("Day"),
            F.col("Hour"),
            F.col("Minute"),
            F.lit(0),
        ),
    )

    return typed_df.drop("Unnamed: 18").filter(F.col("event_timestamp").isNotNull())


def prepare_solar(spark: SparkSession, path: str | Path) -> DataFrame:
    raw_df = read_solar_csv(spark=spark, path=path)
    return clean_solar(raw_df)


def write_solar(df: DataFrame, path: str | Path) -> None:
    df.write.mode("overwrite").parquet(str(path))