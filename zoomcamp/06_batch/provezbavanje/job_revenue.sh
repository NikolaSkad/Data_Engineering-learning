# setuje na lokalni python iz venv-a u de-learning, ne na globalni
source ~/Desktop/DE_Praksa/de-learning/.venv/bin/activate
# treba da ispise lokalni python
which python 

# setuje promenljive ($PWD koristimo samo kada znamo da smo u folderu, zato ide cd sa aps. putanjom za svaki slucaj)
cd ~/Desktop/DE_Praksa/de-learning/
export SPARK_HOME=$PWD/.venv/lib/python3.14/site-packages/pyspark

# provere, da li je,java 17, da li vraca spark home path koji smo postavili i da li je tu spark-daemon.sh
java -version
echo $SPARK_HOME
ls $SPARK_HOME/sbin

# MinIO
docker compose -f zoomcamp/06_batch/minio/docker-compose.yml up -d

# pokretanje Spark master-a
$SPARK_HOME/sbin/spark-daemon.sh start org.apache.spark.deploy.master.Master 1 --host localhost --port 7077 --webui-port 8080
# pokretanje worker-a i dodeljuje mu se spark master adresa (spark://localhost:7077, vidi se na webui pod "URL:" na http://localhost:8080)
$SPARK_HOME/sbin/spark-daemon.sh start org.apache.spark.deploy.worker.Worker 1 spark://localhost:7077

# kreiranje S3 bucket-a pomocu minio-a
aws s3api create-bucket --bucket nyc-tlc --profile minio
# slanje lokalnih parquet fajlova u bucket (sync salje samo ono sto fali ili se promenilo)
aws s3 sync zoomcamp/06_batch/data/pq/ s3://nyc-tlc/pq --profile minio

# slanje job-a na klaster
# --master, --packages i --conf idu u komandu, a ne u kod: skripta opisuje STA radi (citaj, spoji, saberi, upisi),
# da ne bi morali ovo u kodu da kucamo:
# spark = (
#     SparkSession.builder
#     .master("spark://localhost:7077")                                        # = --master
#     .appName("job_revenue")
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0")     # = --packages
#     .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")         # = --conf ...
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
#     .config("spark.hadoop.fs.s3a.path.style.access", "true")
#     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#     .getOrCreate()
# )

# time smo dobili skriptu koja radi u svim okruzenjima, a menja je samo komanda kojom se pokrece

# --packages hadoop-aws:3.5.0      konektor da Spark moze da cita/pise u bucket s3a:// (verzija = Hadoop iz pyspark-a)
# --conf ...endpoint               adresa MinIO-a, inace ide na pravi AWS
# --conf ...access.key/secret.key  korisnik i lozinka za MinIO
# --conf ...path.style.access      MinIO trazi adrese localhost:9000/bucket/...
# --conf ...ssl.enabled=false      MinIO lokalno radi bez HTTPS-a

spark-submit \
    --master spark://localhost:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.5.0 \
    --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    zoomcamp/06_batch/provezbavanje/job_revenue.py \
        --input-green=s3a://nyc-tlc/pq/green/2020/ \
        --input-yellow=s3a://nyc-tlc/pq/yellow/2020/ \
        --output=s3a://nyc-tlc/report-2020

# provera da li je upisano na bucket tako sto citamo iz bucketa
aws s3 ls s3://nyc-tlc/report-2020/ --profile minio

$SPARK_HOME/sbin/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 1
$SPARK_HOME/sbin/spark-daemon.sh stop org.apache.spark.deploy.master.Master 1

docker compose -f zoomcamp/06_batch/minio/docker-compose.yml down