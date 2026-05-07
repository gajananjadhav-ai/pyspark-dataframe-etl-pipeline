# pyspark-dataframe-etl-pipeline

## Project Overview

This project implements PySpark DataFrame APIs by converting SQL-based transformations into programmatic DataFrame transformations and building a modular ETL framework.

Dataset used:

NYC Taxi Trip Data

This project covers:

* SparkSession configuration
* Schema-driven ingestion
* DataFrame transformations
* Joins
* Window functions
* Spark SQL integration
* Performance optimization
* Delta Lake
* Structured Streaming
* Modular ETL architecture

---

## Assignment Coverage

### Part A – PySpark Basics

* SparkSession with custom configuration
* Load Parquet with explicit schema
* Load CSV with manual schema
* Pandas ↔ Spark DataFrame conversion
* JDBC PostgreSQL integration
* Corrupt JSON handling (PERMISSIVE mode)

### Part B – Transformations

* Select, Filter, WithColumn pipeline
* Null handling
* Duplicate removal
* Aggregations
* Explode
* Conditional categorization
* Chained transformations
* UDF implementation

### Part C – Joins

* Inner Join
* Left Join
* Broadcast Join
* Anti Join
* Multi-table Join

### Part D – Window Functions

* Window specification
* Row Number
* Rank
* Dense Rank
* Lag
* Running total
* Percent Rank

### Part E – Advanced Operations

* Temp View + Spark SQL
* Repartition vs Coalesce benchmark
* Cache benchmark
* Delta Lake write
* Structured Streaming
* Modular ETL pipeline

---

## Tech Stack

* Python
* Apache Spark
* PySpark
* Delta Lake
* MySQL
* Pandas
* YAML
* Pytest

---

## Project Structure

```text
pyspark-dataframe-etl-pipeline/
│   config.yaml
│   main.py
│   README.md
│
├── data
│   │   trips.json
│   │   yellow_tripdata_2023-01.parquet
│   │
│   └── stream
│       │   trips_stream.json
│       │   new-trips.json
│
├── etl
│   │   extract.py
│   │   jdbc_extract.py
│   │   load.py
│   │   stream.py
│   │   transform.py
│   │
│   └── __pycache__
│
├── output
│   ├── delta_trips
│   │   ├── _delta_log
│   │   └── trip_date=YYYY-MM-DD
│   │
│   ├── final_trips
│   │
│   └── yellow_csv
│
├── tests
│   │   test_transform.py
│
└── utils
    │   spark_session.py
    │
    └── __pycache__
```

---

## Data Pipeline Architecture

Batch Pipeline:

Extract → Transform → Load

Streaming Pipeline:

Read Stream → Window Aggregation → Console Sink

---

## Implemented Modules

## extract.py

Responsible for batch extraction.

Functions:

* load_parquet()
* load_csv()
* load_json_permissive()
* extract_data()

---

## jdbc_extract.py

Responsible for database extraction.

Functions:

* load_postgres_table()

Features:

* JDBC connection
* Parallel partitioned read
* PostgreSQL integration

---

## transform.py

Responsible for all transformation logic.

Functions include:

* transform_trips()
* handle_nulls()
* remove_duplicates()
* aggregate_trips()
* explode_passengers()
* categorize_trips()
* chain_transformations()
* apply_discount()
* joins
* window functions
* SQL integration
* benchmarking
* delta writing

---

## load.py

Responsible for writing output.

Functions:

* write_csv()

Output:

```text
output/yellow_csv/
```

Single file output using:

coalesce(1)

---

## stream.py

Responsible for streaming ingestion and aggregation.

Functions:

* read_streaming_data()
* windowed_trip_count()
* write_stream_output()

---

## Spark Configuration

Configured in:

utils/spark_session.py

Configurations:

Executor Memory:

```text
4GB
```

Shuffle Partitions:

```text
200
```

---

## Performance Benchmarks

## Cache Benchmark

Observation:

First execution slower

Second execution faster

Reason:

Data reused from memory cache.

---

## Repartition vs Coalesce

Repartition:

* Causes full shuffle
* Used to increase/decrease partitions

Coalesce:

* Avoids full shuffle
* Best for reducing partitions

Benchmark:

100 partitions → 10 partitions

---

## Structured Streaming

Streaming Source:

```text
data/stream/
```

Window Duration:

```text
5 minutes
```

Output Mode:

```text
complete
```

Sink:

```text
console
```

Example Output:

```text
Batch: 0

Window 10:00–10:05 → 3 trips
Window 10:05–10:10 → 1 trip
```

Live Testing:

Adding:

```text
new-trips.json
```

creates next batch automatically.

---

## Delta Lake Output

Location:

```text
output/delta_trips/
```

Partitioned By:

```text
trip_date
```

Benefits:

* ACID transactions
* Schema enforcement
* Time travel
* Faster reads

---

## Final Batch Output

CSV:

```text
output/yellow_csv/
```

Parquet:

```text
output/final_trips/
```

Delta:

```text
output/delta_trips/
```

---

## How to Run

Install dependencies:

```bash
pip install pyspark pandas delta-spark pyyaml pytest
```

Run main pipeline:

```bash
python main.py
```

Run tests:

```bash
pytest tests/test_transform.py
```

---

## Topics Covered

PySpark Core:

* DataFrame API
* Lazy evaluation
* Actions vs Transformations

Optimization:

* Cache vs Persist
* Repartition vs Coalesce
* Broadcast Join

Advanced:

* Window Functions
* Structured Streaming
* Delta Lake
* JDBC ingestion

Architecture:

* Modular ETL Design
* Batch + Streaming integration

---

## Real-World Concepts Learned

* Schema management
* Data quality handling
* Distributed joins
* Parallel database ingestion
* Streaming micro-batches
* Delta architecture
* Production-style ETL

---

## Author

Gajanan Jadhav

Aspiring Data Engineer | MIS Executive
