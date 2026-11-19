# Data Engineering Learning

This repo holds my hands-on data engineering work. It has batch and streaming pipelines built with Spark, Kafka and Flink. Everything runs locally in Docker, with no cloud account needed.

## Tech stack

- **Processing:** PySpark (DataFrame, Spark SQL, RDD), Spark Structured Streaming, PyFlink
- **Streaming:** Apache Kafka, Kafka Streams (Java), Redpanda
- **Storage:** HDFS, Hive, MinIO (S3 compatible), PostgreSQL, Parquet
- **Infra:** Docker, Docker Compose, Spark standalone cluster, Hadoop
- **Code quality:** Python, uv, Ruff, mypy, Jupyter

## Highlights

### 1. Batch revenue job on NYC taxi data

A PySpark job reads green and yellow NYC taxi trips (Parquet), unions them and builds a monthly revenue report per pickup zone with Spark SQL. The job takes input and output paths as arguments, so the same code runs locally or on a cluster. It runs with `spark-submit` on a Spark cluster in Docker Compose (1 master, 2 workers). MinIO acts as S3 storage for input and output (`s3a://`). This replaces the Google Cloud Dataproc and GCS setup from the course.

Tools: PySpark, Spark SQL, spark-submit, Docker Compose, MinIO, AWS CLI

Folders: [`zoomcamp/06_batch/provezbavanje`](zoomcamp/06_batch/provezbavanje) (my job written from scratch), [`zoomcamp/06_batch/15_local_dataproc`](zoomcamp/06_batch/15_local_dataproc)

### 2. Streaming: Kafka producer and consumer, Flink job to PostgreSQL

A Python producer reads NYC taxi trips and sends them as JSON to a Kafka topic (Redpanda broker). A Python consumer reads the topic and writes each ride to PostgreSQL. A PyFlink job does the same work inside Flink, with checkpoints every 10 seconds.

Tools: Python, kafka-python, Redpanda, PyFlink, PostgreSQL, Docker Compose

Folder: [`zoomcamp/07_streaming`](zoomcamp/07_streaming)

### 3. Live Bluesky posts: Kafka, Spark Structured Streaming, HDFS

A Python producer reads live Bluesky posts and sends them to Kafka. A Spark Structured Streaming job counts words (without stop words) and writes the counts back to Kafka. A second Spark job reads the counts, uses a 1 minute window with a watermark and saves the results to HDFS as Parquet. Kafka, Spark, HDFS and Hue all run in Docker Compose.

Tools: Spark Structured Streaming, Kafka, HDFS, Hue, Docker Compose

Folder: [`praksa/Spark_Structured_Streaming_resenja`](praksa/Spark_Structured_Streaming_resenja)

## Repo structure

```
praksa/       exercises from the ASVSP course (big data systems)
  Docker_resenja/                      Docker images, networks, volumes
  Docker_compose_resenja/              multi-service apps with Docker Compose
  HDFS_resenja/                        Hadoop, HDFS and Hive in Docker
  Spark_resenja/                       PySpark DataFrame, RDD, window functions, JDBC to PostgreSQL
  Kafka_resenja/                       Kafka notes and exercises
  Kafka_Stream_resenja/                Kafka Streams in Java (flight data)
  Spark_Structured_Streaming_resenja/  Spark streaming with Kafka and HDFS
zoomcamp/     work from the DataTalksClub Data Engineering Zoomcamp
  06_batch/                            PySpark batch processing, local Spark cluster, MinIO
  07_streaming/                        Kafka producer and consumer, PyFlink
moderni_stack_beleske.md               notes: course stack compared to the modern data stack
```

## How to run

You need Docker Desktop.

### Batch job

1. Download the taxi data with [`download-data.sh`](zoomcamp/06_batch/download-data.sh) and convert it to Parquet ([`05_taxi_schema.ipynb`](zoomcamp/06_batch/05_taxi_schema.ipynb)). The data goes to `zoomcamp/06_batch/data/pq`.
2. Start MinIO, the Spark cluster and the submit container. A helper container uploads the data to MinIO.

```bash
cd zoomcamp/06_batch/provezbavanje
docker volume create minio_minio_data
docker compose up -d
```

3. Run the job for one year and stop everything.

```bash
docker compose exec submit bash /opt/app/submit.sh 2021
docker compose down
```

The report is written to `s3a://nyc-tlc/report-2021` in MinIO (console at http://localhost:9001).

### Streaming pipeline

```bash
cd zoomcamp/07_streaming
docker compose up -d
```

1. Create the `processed_events` table in PostgreSQL.
2. Run [`notebook/producer.ipynb`](zoomcamp/07_streaming/notebook/producer.ipynb) to send rides to Kafka.
3. Read them with [`notebook/consumer.ipynb`](zoomcamp/07_streaming/notebook/consumer.ipynb), or submit the Flink job:

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/pass_through_job.py \
    --pyFiles /opt/src -d
```

## Next steps

- Airflow for orchestration
- dbt for SQL transformations with tests
- Delta Lake or Apache Iceberg as a table format
- Cloud (most likely AWS)
