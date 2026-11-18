#!/usr/bin/env bash
# Lokalna zamena za lekciju 06-batch/15 (Dataproc), bez Google Cloud-a:
#   Dataproc klaster  -> Spark standalone (master + worker) iz .venv-a
#   GCS bucket (gs://) -> MinIO u Docker-u (s3a://)
#   gcloud dataproc jobs submit pyspark -> spark-submit
#
# Upotreba (iz bilo kog foldera):
#   ./local_dataproc.sh all              # sve odjednom: start + upload + submit/check za 2020 i 2021 + stop
#   ./local_dataproc.sh all 2021         # isto, samo za navedene godine
#
#   ./local_dataproc.sh start            # MinIO + master + worker
#   ./local_dataproc.sh upload           # data/pq -> s3a://nyc-tlc/pq (samo ono što fali)
#   ./local_dataproc.sh submit 2021      # pokreni 06_spark_sql.py za godinu, izlaz na MinIO
#   ./local_dataproc.sh check 2021       # pogledaj izlaz u bucket-u
#   ./local_dataproc.sh stop             # ugasi sve
#
# Preduslovi: Docker Desktop, aws CLI sa profilom "minio" (vidi ../minio/), Java 17.
# Job je ../06_spark_sql.py (isti kao u lekciji 06); NE sme da ima .master(...) u kodu: master dolazi iz spark-submit-a.

set -euo pipefail

LESSON_DIR="$(cd "$(dirname "$0")" && pwd)"
ZOOMCAMP_DIR="$(dirname "$LESSON_DIR")" # zoomcamp/06_batch
REPO_DIR="$(dirname "$(dirname "$ZOOMCAMP_DIR")")"

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17)}"
export SPARK_HOME="$REPO_DIR/.venv/lib/python3.14/site-packages/pyspark"
export PATH="$REPO_DIR/.venv/bin:$PATH"

MASTER_URL="spark://localhost:7077"
BUCKET="nyc-tlc"

MASTER_CLASS="org.apache.spark.deploy.master.Master"
WORKER_CLASS="org.apache.spark.deploy.worker.Worker"

start() {
    # 1. MinIO (S3 API na :9000, konzola na :9001, minioadmin / minioadmin)
    docker compose -f "$ZOOMCAMP_DIR/minio/docker-compose.yml" up -d

    # 2. Master (UI na :8080) i jedan worker koji se prijavi masteru.
    #    pip pyspark nema start-master.sh / start-worker.sh, pa se zove spark-daemon.sh direktno.
    #    "|| true": ako već rade, spark-daemon.sh to javi i ide se dalje.
    "$SPARK_HOME/sbin/spark-daemon.sh" start "$MASTER_CLASS" 1 \
        --host localhost --port 7077 --webui-port 8080 || true
    "$SPARK_HOME/sbin/spark-daemon.sh" start "$WORKER_CLASS" 1 "$MASTER_URL" || true

    echo "Master UI: http://localhost:8080  |  MinIO konzola: http://localhost:9001"
}

upload() {
    # Kao "gsutil -m cp -r pq/ gs://..." iz kursa; sync šalje samo fajlove kojih nema u bucket-u.
    aws s3 mb "s3://$BUCKET" --profile minio 2>/dev/null || true
    aws s3 sync "$ZOOMCAMP_DIR/data/pq/" "s3://$BUCKET/pq" --profile minio
}

submit() {
    local year="${1:?navedi godinu, npr. ./local_dataproc.sh submit 2021}"

    # 3. Posao ide na klaster preko spark-submit-a (kao "Submit job" na Dataproc-u).
    #    hadoop-aws je S3 konektor (verzija = Hadoop iz pyspark-a, 3.5.0),
    #    a s3a podešavanja idu kroz --conf, ne kroz kod, pa skripta radi i lokalno i na cloud-u.
    cd "$ZOOMCAMP_DIR"
    spark-submit \
        --master "$MASTER_URL" \
        --packages org.apache.hadoop:hadoop-aws:3.5.0 \
        --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        06_spark_sql.py \
            --input_green="s3a://$BUCKET/pq/green/$year/" \
            --input_yellow="s3a://$BUCKET/pq/yellow/$year/" \
            --output="s3a://$BUCKET/report-$year"
}

check() {
    local year="${1:?navedi godinu, npr. ./local_dataproc.sh check 2021}"
    # Uspešan posao ostavi _SUCCESS i part-*.parquet fajlove.
    aws s3 ls "s3://$BUCKET/report-$year/" --profile minio
}

all() {
    # Ceo tok jednim pozivom: klaster gore, podaci u bucket, po jedan job za svaku godinu, pa sve ugasi.
    local years=("$@")
    [ ${#years[@]} -eq 0 ] && years=(2020 2021)

    start
    sleep 5 # da se worker prijavi masteru
    upload
    for year in "${years[@]}"; do
        submit "$year"
        check "$year"
    done

    stop
}

stop() {
    # Prvo worker, pa master, pa MinIO (podaci ostaju u Docker volume-u).
    "$SPARK_HOME/sbin/spark-daemon.sh" stop "$WORKER_CLASS" 1 || true
    "$SPARK_HOME/sbin/spark-daemon.sh" stop "$MASTER_CLASS" 1 || true
    docker compose -f "$ZOOMCAMP_DIR/minio/docker-compose.yml" down
}

case "${1:-}" in
    all)    shift; all "$@" ;;
    start)  start ;;
    upload) upload ;;
    submit) submit "${2:-}" ;;
    check)  check "${2:-}" ;;
    stop)   stop ;;
    *)
        echo "Upotreba: $0 {all [godine...]|start|upload|submit <godina>|check <godina>|stop}"
        exit 1
        ;;
esac
