def write_csv(df):
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("output/yellow_csv")
    )