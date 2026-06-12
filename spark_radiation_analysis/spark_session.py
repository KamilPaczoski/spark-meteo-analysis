from pyspark.sql import SparkSession


# Lokalna sesja sprakowa
def create_spark_session(app_name: str = "solar-radiation-analysis") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
