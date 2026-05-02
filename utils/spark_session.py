from pyspark.sql import SparkSession
import yaml


def create_spark_session():
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    spark = SparkSession.builder \
        .appName(config["spark"]["app_name"]) \
        .config("spark.executor.memory", config["spark"]["executor_memory"]) \
        .config("spark.sql.shuffle.partitions", config["spark"]["shuffle_partitions"]) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .getOrCreate()

    return spark