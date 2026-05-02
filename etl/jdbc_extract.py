def load_postgres_table(spark):
    df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/nyc_taxi") \
        .option("dbtable", "trips") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .option("partitionColumn", "pickup_date") \
        .option("lowerBound", "1") \
        .option("upperBound", "31") \
        .option("numPartitions", "4") \
        .load()

    return df