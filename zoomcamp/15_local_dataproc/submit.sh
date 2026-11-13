#!/usr/bin/env bash
# Pokreće se u "submit" kontejneru: docker compose run --rm submit <godina>
set -euo pipefail

YEAR="${1:?navedi godinu, npr. docker compose run --rm submit 2021}"

# Driver radi u ovom kontejneru, a executor-i na worker-ima mu se javljaju nazad preko ove IP adrese.
DRIVER_HOST="$(hostname -i | awk '{print $1}')"

/opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.driver.host="$DRIVER_HOST" \
    --conf spark.jars.ivy=/opt/ivy \
    --packages org.apache.hadoop:hadoop-aws:3.5.0 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    /opt/app/06_spark_sql.py \
        --input_green="s3a://nyc-tlc/pq/green/$YEAR/" \
        --input_yellow="s3a://nyc-tlc/pq/yellow/$YEAR/" \
        --output="s3a://nyc-tlc/report-$YEAR"
