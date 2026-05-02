from pyspark.sql.functions import col, window, count


def read_streaming_data(spark):
    return (
        spark.readStream
        .schema("VendorID INT, tpep_pickup_datetime TIMESTAMP")
        .json("data/stream")
    )


def windowed_trip_count(stream_df):
    return (
        stream_df.groupBy(
            window(
                col("tpep_pickup_datetime"),
                "5 minutes"
            )
        )
        .agg(
            count("*").alias("trip_count")
        )
    )


def write_stream_output(result_df):
    query = (
        result_df.writeStream
        .outputMode("complete")
        .format("console")
        .start()
    )

    query.awaitTermination()