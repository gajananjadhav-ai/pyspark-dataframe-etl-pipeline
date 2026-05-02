from pyspark.sql import Row
import pandas as pd

from utils.spark_session import create_spark_session

from etl.extract import (
    load_parquet,
    load_csv,
    load_json_permissive,
    extract_data
)

from etl.load import write_csv

from etl.transform import (
    transform_trips,
    handle_nulls,
    remove_duplicates,
    aggregate_trips,
    explode_passengers,
    categorize_trips,
    chain_transformations,
    apply_discount,
    join_trips_zones,
    join_trips_payments,
    broadcast_join_trips_zones,
    find_drivers_without_trips,
    create_master_report,
    define_window_spec,
    assign_trip_sequence,
    rank_drivers_by_revenue,
    find_trip_gaps,
    calculate_running_metrics,
    sql_query_on_temp_view,
    compare_repartition_coalesce,
    cache_dataframe,
    write_delta,
    transform_data,
    load_data
)

from etl.stream import (
    read_streaming_data,
    windowed_trip_count,
    write_stream_output
)

spark = create_spark_session()
print(spark)

file_path = r"C:\Users\admin\OneDrive\Desktop\Gajanan\PYSPARK DATAFRAME APIs_ASSN 2\assignment5_pyspark\data\yellow_tripdata_2023-01.parquet"


# ------------------------------
# Batch Pipeline (Q1–Q28)
# ------------------------------

df = load_parquet(spark, file_path)

df.printSchema()
df.show(5)

write_csv(df)

csv_df = load_csv(spark, "output/yellow_csv")
csv_df.show(5)
csv_df.printSchema()

pandas_df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["A", "B", "C", "D", "E"]
})

spark_df = spark.createDataFrame(pandas_df)
converted_back = spark_df.toPandas()

spark_df.show()
print(converted_back)

transformed_df = transform_trips(df)
transformed_df.show(5)
transformed_df.printSchema()

json_df = load_json_permissive(spark, "data/trips.json")
json_df.show(truncate=False)
json_df.printSchema()

null_handled_df = handle_nulls(df)
null_handled_df.show(5)

dedup_df = remove_duplicates(df)
dedup_df.show(5)

agg_df = aggregate_trips(df)
agg_df.show()

exploded_df = explode_passengers(df)

exploded_df.select(
    "VendorID",
    "store_and_fwd_flag",
    "passenger_id"
).show(5)

categorized_df = categorize_trips(df)

categorized_df.select(
    "trip_distance",
    "trip_category"
).show(5)

chained_df = chain_transformations(df)
chained_df.show(5)

chained_df.explain()

discounted_df = apply_discount(df)

discounted_df.select(
    "trip_distance",
    "discounted_fare"
).show(5)

zones_data = [
    Row(VendorID=1, zone="Manhattan"),
    Row(VendorID=2, zone="Brooklyn")
]

zones_df = spark.createDataFrame(zones_data)

joined_df = join_trips_zones(df, zones_df)

joined_df.select(
    "t.VendorID",
    "trip_distance",
    "z.zone"
).show(5)

payments_data = [
    Row(VendorID=1, payment_status="Paid")
]

payments_df = spark.createDataFrame(payments_data)

payment_join_df = join_trips_payments(df, payments_df)

payment_join_df.select(
    "t.VendorID",
    "trip_distance",
    "p.payment_status"
).show(5)

broadcast_df = broadcast_join_trips_zones(df, zones_df)
broadcast_df.show(5)

drivers_data = [
    Row(driver_id=101),
    Row(driver_id=102),
    Row(driver_id=103)
]

drivers_df = spark.createDataFrame(drivers_data)

driver_info_data = [
    Row(driver_id=101, VendorID=1, driver_name="Amit"),
    Row(driver_id=102, VendorID=2, driver_name="Rahul")
]

driver_info_df = spark.createDataFrame(driver_info_data)

trip_driver_data = [
    Row(driver_id=101, trip_id=1),
    Row(driver_id=102, trip_id=2)
]

trip_driver_df = spark.createDataFrame(trip_driver_data)

no_trip_df = find_drivers_without_trips(
    drivers_df,
    trip_driver_df
)

no_trip_df.show()

master_df = create_master_report(
    df,
    driver_info_df,
    zones_df
)

master_df.select(
    "VendorID",
    "driver_name",
    "zone",
    "trip_distance"
).show(5)

window_spec = define_window_spec()
print(window_spec)

sequence_df = assign_trip_sequence(trip_driver_df)
sequence_df.show()

revenue_data = [
    Row(driver_id=101, borough="Manhattan", revenue=500),
    Row(driver_id=102, borough="Manhattan", revenue=400),
    Row(driver_id=103, borough="Brooklyn", revenue=700)
]

revenue_df = spark.createDataFrame(revenue_data)

ranked_df = rank_drivers_by_revenue(revenue_df)
ranked_df.show()

trip_time_data = [
    Row(driver_id=101, trip_id=1, trip_time=10),
    Row(driver_id=101, trip_id=2, trip_time=20),
    Row(driver_id=101, trip_id=3, trip_time=35)
]

trip_time_df = spark.createDataFrame(trip_time_data)

gap_df = find_trip_gaps(trip_time_df)
gap_df.show()

metrics_df = calculate_running_metrics(revenue_df)
metrics_df.show()

sql_result = sql_query_on_temp_view(df, spark)
sql_result.show()

repartitioned_df, coalesced_df = compare_repartition_coalesce(df)

print("Repartition count:", repartitioned_df.rdd.getNumPartitions())
print("Coalesce count:", coalesced_df.rdd.getNumPartitions())

cached_df, first_run, second_run = cache_dataframe(df)

print("First run time:", first_run)
print("Second run time:", second_run)

write_delta(df)
print("Delta write completed")


# ------------------------------
# Modular ETL + Streaming (Q29 + Q30)
# ------------------------------

def main():
    # Batch ETL
    extracted_df = extract_data(spark, file_path)

    transformed_df = transform_data(extracted_df)

    load_data(transformed_df)

    # Streaming ETL
    stream_df = read_streaming_data(spark)

    stream_result = windowed_trip_count(stream_df)

    write_stream_output(stream_result)


if __name__ == "__main__":
    main()