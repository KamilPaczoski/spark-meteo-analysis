from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Schemat
RAW_SOLAR_SCHEMA = StructType(
    [
        StructField("Year", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Day", IntegerType(), True),
        StructField("Hour", IntegerType(), True),
        StructField("Minute", IntegerType(), True),
        StructField("Temperature", DoubleType(), True),
        StructField("Clearsky DHI", DoubleType(), True),
        StructField("Clearsky DNI", DoubleType(), True),
        StructField("Clearsky GHI", DoubleType(), True),
        StructField("Dew Point", DoubleType(), True),
        StructField("DHI", DoubleType(), True),
        StructField("DNI", DoubleType(), True),
        StructField("GHI", DoubleType(), True),
        StructField("Relative Humidity", DoubleType(), True),
        StructField("Solar Zenith Angle", DoubleType(), True),
        StructField("Surface Albedo", DoubleType(), True),
        StructField("Pressure", DoubleType(), True),
        StructField("Wind Speed", DoubleType(), True),
        StructField("Unnamed: 18", StringType(), True),
    ]
)

# Mapowanie nazw headerów z CSV
NORMALIZED_COLUMNS = {
    "Clearsky DHI": "Clearsky_DHI",
    "Clearsky DNI": "Clearsky_DNI",
    "Clearsky GHI": "Clearsky_GHI",
    "Dew Point": "Dew_Point",
    "Relative Humidity": "Relative_Humidity",
    "Solar Zenith Angle": "Solar_Zenith_Angle",
    "Surface Albedo": "Surface_Albedo",
    "Wind Speed": "Wind_Speed",
}
