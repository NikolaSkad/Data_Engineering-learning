# 15: Dataproc lokalno (bez Google Cloud-a)

Lekcija `06-batch/15-setting-up-a-dataproc-cluster.md` traži GCP billing (karticu).
Ovde je isti tok, samo lokalno:

| Kurs (GCP)                                  | Lokalno                                         |
| ------------------------------------------- | ----------------------------------------------- |
| Dataproc klaster                            | Spark standalone: master + worker iz `.venv`-a  |
| GCS bucket `gs://...`                       | MinIO u Docker-u, `s3a://nyc-tlc/...`           |
| `gsutil -m cp -r pq/ gs://...`              | `aws s3 sync ... --profile minio`               |
| `gcloud dataproc jobs submit pyspark`       | `spark-submit --master spark://localhost:7077`  |

Job je `../06_spark_sql.py` (bez `.master()` u kodu). Ulaz i izlaz se prosleđuju kao argumenti.

## Pokretanje

```bash
./local_dataproc.sh all          # start + upload + report-2020 i report-2021
./local_dataproc.sh stop         # ugasi worker, master i MinIO
```

Ili korak po korak: `start`, `upload`, `submit <godina>`, `check <godina>`, `stop`.

- Master UI: http://localhost:8080 (završeni poslovi su pod "Completed Applications")
- MinIO konzola: http://localhost:9001 (`minioadmin` / `minioadmin`)
- Izlaz: `s3a://nyc-tlc/report-<godina>/` (`_SUCCESS` + `part-*.parquet`)

## Preduslovi

- Docker Desktop (MinIO, `../minio/docker-compose.yml`)
- `aws` CLI (`uv tool install awscli`) sa profilom `minio` (endpoint `http://localhost:9000`)
- Java 17, pyspark iz `.venv`-a
