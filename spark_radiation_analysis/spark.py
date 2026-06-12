from __future__ import annotations

import os

from pyspark.sql import SparkSession


def create_spark(app_name: str = "spark-meteo-analysis") -> SparkSession:
    master = os.getenv("SPARK_MASTER", "local[*]")
    partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark