from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType
)

trip_schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True)
])


def load_parquet(spark, file_path):
    df = spark.read.schema(trip_schema).parquet(file_path)
    return df


def load_csv(spark, file_path):
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", False) \
        .schema(trip_schema) \
        .csv(file_path)

    return df


def load_json_permissive(spark, file_path):
    df = spark.read \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .json(file_path)

    return df


def extract_data(spark, file_path):
    df = load_parquet(spark, file_path)
    return df