import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            ".."
        )
    )
)

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType
)

from etl.transform import (
    handle_nulls,
    remove_duplicates,
    transform_data
)

spark = SparkSession.builder \
    .appName("TestTransform") \
    .master("local[*]") \
    .getOrCreate()


def test_handle_nulls():
    data = [
        (1, None, None, None)
    ]

    schema = StructType([
        StructField("VendorID", LongType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    result = handle_nulls(df)

    row = result.collect()[0]

    assert row["passenger_count"] == 0
    assert row["trip_distance"] == 0
    assert row["store_and_fwd_flag"] == "Unknown"


def test_remove_duplicates():
    data = [
        (1, 2.5),
        (1, 2.5)
    ]

    columns = [
        "VendorID",
        "trip_distance"
    ]

    df = spark.createDataFrame(data, columns)

    result = remove_duplicates(df)

    assert result.count() == 1


def test_transform_data():
    data = [
        (1, 3.0),
        (2, 0.5)
    ]

    columns = [
        "VendorID",
        "trip_distance"
    ]

    df = spark.createDataFrame(data, columns)

    result = transform_data(df)

    assert result.count() == 1
    assert "distance_km" in result.columns


if __name__ == "__main__":
    test_handle_nulls()
    test_remove_duplicates()
    test_transform_data()
    print("All tests passed successfully")