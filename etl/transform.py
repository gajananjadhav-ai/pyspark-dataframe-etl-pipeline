from pyspark.sql.functions import (
    col, count, sum, avg, min, max,
    split, explode, when, udf, broadcast,
    row_number, rank, dense_rank, lag,
    percent_rank, current_date
)
from pyspark.sql.window import Window
import time


def transform_trips(df):
    return df.select(
        "VendorID",
        "passenger_count",
        "trip_distance"
    ).filter(
        col("trip_distance") > 1
    ).withColumn(
        "distance_km",
        col("trip_distance") * 1.60934
    ).withColumnRenamed(
        "VendorID",
        "vendor_id"
    ).drop(
        "passenger_count"
    )


def handle_nulls(df):
    return df.fillna({
        "passenger_count": 0,
        "trip_distance": 0,
        "store_and_fwd_flag": "Unknown"
    })


def remove_duplicates(df):
    return df.dropDuplicates([
        "VendorID",
        "trip_distance"
    ])


def aggregate_trips(df):
    return df.groupBy("VendorID").agg(
        count("*").alias("trip_count"),
        sum("trip_distance").alias("total_distance"),
        avg("trip_distance").alias("avg_distance"),
        min("trip_distance").alias("min_distance"),
        max("trip_distance").alias("max_distance")
    )


def explode_passengers(df):
    return df.withColumn(
        "passenger_ids",
        split(col("store_and_fwd_flag"), "")
    ).withColumn(
        "passenger_id",
        explode(col("passenger_ids"))
    )


def categorize_trips(df):
    return df.withColumn(
        "trip_category",
        when(col("trip_distance") < 2, "Short Trip")
        .when(col("trip_distance") < 5, "Medium Trip")
        .otherwise("Long Trip")
    )


def chain_transformations(df):
    return df.select(
        "VendorID",
        "passenger_count",
        "trip_distance"
    ).filter(
        col("trip_distance") > 1
    ).withColumn(
        "distance_km",
        col("trip_distance") * 1.60934
    ).withColumnRenamed(
        "VendorID",
        "vendor_id"
    ).drop(
        "passenger_count"
    ).filter(
        col("distance_km") > 2
    )


def apply_discount(df):
    def discount_logic(fare):
        if fare < 100:
            return fare * 0.95
        return fare * 0.90

    discount_udf = udf(discount_logic)

    return df.withColumn(
        "discounted_fare",
        discount_udf(col("trip_distance"))
    )


def join_trips_zones(trips_df, zones_df):
    return trips_df.alias("t").join(
        zones_df.alias("z"),
        col("t.VendorID") == col("z.VendorID"),
        "inner"
    )


def join_trips_payments(trips_df, payments_df):
    return trips_df.alias("t").join(
        payments_df.alias("p"),
        col("t.VendorID") == col("p.VendorID"),
        "left"
    )


def broadcast_join_trips_zones(trips_df, zones_df):
    return trips_df.join(
        broadcast(zones_df),
        "VendorID"
    )


def find_drivers_without_trips(drivers_df, trip_driver_df):
    return drivers_df.join(
        trip_driver_df,
        "driver_id",
        "left_anti"
    )


def create_master_report(trips_df, driver_info_df, zones_df):
    return trips_df.join(
        driver_info_df,
        "VendorID",
        "inner"
    ).join(
        zones_df,
        "VendorID",
        "inner"
    )


def define_window_spec():
    return Window.partitionBy(
        "driver_id"
    ).orderBy(
        "trip_id"
    )


def assign_trip_sequence(trip_driver_df):
    window_spec = define_window_spec()

    return trip_driver_df.withColumn(
        "trip_sequence",
        row_number().over(window_spec)
    )


def rank_drivers_by_revenue(revenue_df):
    window_spec = Window.partitionBy(
        "borough"
    ).orderBy(
        col("revenue").desc()
    )

    return revenue_df.withColumn(
        "rank",
        rank().over(window_spec)
    ).withColumn(
        "dense_rank",
        dense_rank().over(window_spec)
    )


def find_trip_gaps(trip_time_df):
    window_spec = Window.partitionBy(
        "driver_id"
    ).orderBy(
        "trip_id"
    )

    return trip_time_df.withColumn(
        "previous_trip_time",
        lag("trip_time").over(window_spec)
    ).withColumn(
        "gap",
        col("trip_time") - col("previous_trip_time")
    )


def calculate_running_metrics(revenue_df):
    window_spec = Window.partitionBy(
        "borough"
    ).orderBy(
        "revenue"
    )

    return revenue_df.withColumn(
        "running_total",
        sum("revenue").over(window_spec)
    ).withColumn(
        "percent_rank",
        percent_rank().over(window_spec)
    )


def sql_query_on_temp_view(df, spark):
    df.createOrReplaceTempView("trips_view")

    return spark.sql("""
        SELECT VendorID, COUNT(*) AS total_trips
        FROM trips_view
        GROUP BY VendorID
    """)


def compare_repartition_coalesce(df):
    repartitioned_df = df.repartition(100)
    coalesced_df = repartitioned_df.coalesce(10)

    return repartitioned_df, coalesced_df


def cache_dataframe(df):
    intermediate_df = df.filter(
        "trip_distance > 2"
    )

    start = time.time()
    intermediate_df.count()
    first_run = time.time() - start

    cached_df = intermediate_df.cache()
    cached_df.count()

    start = time.time()
    cached_df.count()
    second_run = time.time() - start

    return cached_df, first_run, second_run


def write_delta(df):
    df = df.withColumn(
        "trip_date",
        current_date()
    )

    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("trip_date") \
        .save("output/delta_trips")


def transform_data(df):
    return df.filter(
        col("trip_distance") > 1
    ).withColumn(
        "distance_km",
        col("trip_distance") * 1.60934
    )


def load_data(df):
    df.write \
        .mode("overwrite") \
        .parquet("output/final_trips")