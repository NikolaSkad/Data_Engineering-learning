# Big data stack: kurs vs. moderni stack (2026)

Poređenje tehnologija iz ASVSP kursa (lekcije 01–06) sa onim što se danas koristi.

| Oblast                     | Kurs (≈ 2015–2020)                        | Moderni stack (2026)                                                 | Šta se promenilo                                                                               | Da li se staro još koristi?                  |
| -------------------------- | ----------------------------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------------- |
| **Skladište**              | HDFS (namenode + datanode)                | **S3 / MinIO / GCS / Azure Blob**                                    | Skladište je odvojeno od obrade: plaćaš prostor, a ne server koji stalno radi                  | Samo u starim on-prem sistemima              |
| **Format tabela**          | Hive tabele nad parquet/ORC               | **Delta Lake / Apache Iceberg** (nad parquet-om)                     | Dodaje ACID transakcije, `UPDATE`/`MERGE`, istoriju verzija (time travel) i evoluciju šeme     | Parquet da, "gole" Hive tabele sve manje     |
| **Metapodaci (katalog)**   | Hive Metastore                            | **Unity Catalog, AWS Glue, Iceberg REST katalog**                    | Centralno upravljanje pristupom i lineage podataka                                             | Hive Metastore još radi ispod mnogih sistema |
| **Batch obrada**           | Spark 3.0 (RDD, DataFrame), MapReduce     | **Spark 3.5 / 4.x** (DataFrame, SQL), Databricks                     | RDD i MapReduce skoro niko ne piše ručno, sve je DataFrame i SQL                               | Spark da (i dalje standard), MapReduce ne    |
| **SQL nad jezerom**        | Hive (HiveQL na MapReduce/Tez)            | **Trino / Presto, Spark SQL, DuckDB**                                | Interaktivni upiti za sekunde umesto minuta                                                    | Hive kao engine retko                        |
| **Transformacije**         | Spark skripte, HiveQL                     | **dbt** (SQL + testovi + dokumentacija), Spark                       | Transformacije kao verzionisan SQL kod, sa testovima                                           | Spark skripte i dalje                        |
| **Tokovi (broker)**        | Kafka 2.x + ZooKeeper                     | **Kafka 4 (KRaft)**, Redpanda, Confluent Cloud                       | Nema ZooKeeper-a, a sve više se koristi kao managed servis                                     | Kafka da (standard), ZooKeeper ne            |
| **Obrada tokova**          | Kafka Streams, Spark Structured Streaming | **Apache Flink**, Spark Structured Streaming, Kafka Streams          | Flink je postao glavni izbor za streaming sa malim kašnjenjem                                  | Oba i dalje                                  |
| **Orkestracija**           | (nije bilo, ručne skripte)                | **Airflow**, Dagster, Prefect                                        | Pipeline kao kod: zavisnosti, ponavljanje, raspored, monitoring                                | —                                            |
| **Učitavanje (ingestion)** | `hdfs dfs -put`, ručne skripte            | **Airbyte, Fivetran, Kafka Connect**, Airflow                        | Gotovi konektori za stotine izvora                                                             | Skripte i dalje za custom izvore             |
| **Vizualizacija**          | (nije bilo) / Hue                         | **Metabase, Superset, Power BI, Tableau, Looker**                    | BI alati direktno nad jezerom ili bazom                                                        | Hue skoro ne                                 |
| **Infrastruktura**         | Docker Compose, sopstveni klaster         | **Kubernetes, cloud (AWS / GCP / Azure)**, Databricks, Snowflake     | Managed servisi umesto održavanja klastera                                                     | Docker Compose za lokalni razvoj da          |
| **Arhitektura**            | Data lake (zone na HDFS-u)                | **Lakehouse** (lake + Delta/Iceberg), medallion (bronze/silver/gold) | Jezero sa osobinama skladišta podataka. Raw/transform/curated su isto što i bronze/silver/gold | Ideja zona je ista, samo se drugačije zove   |

## Šta mi ovo znači

- **Koncepti iz kursa i dalje važe:** particije, formati, watermark, window, offseti, consumer grupe. Menjaju se alati, a ne ideje.
- **Spark i Kafka su i dalje standard.** Ono što sam naučio direktno se prenosi.
- **Zone iz projekta (raw / transformation / curated)** su isto što i "medallion" arhitektura (bronze / silver / gold).
- **Najveće praznine u odnosu na oglase za posao:** Airflow, dbt, Delta/Iceberg i cloud (najčešće AWS). Redosled učenja posle projekta.

## Plan učenja: PySpark, Kafka 4, S3 (MinIO), Docker Compose

### 1. Glavni kurs: DataTalksClub Data Engineering Zoomcamp (besplatno)

<https://github.com/DataTalksClub/data-engineering-zoomcamp>

Besplatan kurs sa video lekcijama, domaćim zadacima i rešenjima. Moduli:

- Docker i Compose (od nule),
- orkestracija (Kestra ili Airflow, zavisi od godine),
- data warehouse,
- **batch obrada: PySpark** (DataFrame, SQL, `groupBy`, `join`, window),
- **streaming: Kafka** (producer, consumer, streaming obrada),
- završni projekat, vrlo sličan ASVSP projektu.

Oni koriste Google Cloud Storage umesto S3-a. Kod je skoro isti, menja se samo putanja (`gs://` → `s3a://`), pa sa MinIO-om sve radi lokalno.

### 2. Zadaci za Spark i window funkcije

- **Databricks "Learning Spark, 2nd Edition"** (2020, Spark 3, besplatna PDF verzija na Databricks sajtu) i primeri na <https://github.com/databricks/LearningSparkV2>. Knjiga u `Knjige/` je prvo izdanje iz 2015 (Spark 1.x), zastarela.
- **Window funkcije:** zadaci na **DataLemur** (datalemur.com) ili **StrataScratch**. SQL zadaci sa intervjua, rešavaju se u Spark SQL-u ili DataFrame API-ju, window funkcije su iste.

### 3. Kafka

- **Confluent Developer** (developer.confluent.io): kursevi "Apache Kafka 101" i tutorijali, ažurni za KRaft.
- **`Knjige/confluent-kafka-definitive-guide-complete.pdf`:** koncepti (particije, consumer grupe, replikacija) važe i dalje, preskočiti poglavlja o ZooKeeper-u.

### 4. S3 / MinIO + Spark

Nema posebnog kursa, to je samo podešavanje (`s3a://` pristup u compose fajlu). Čitanje i pisanje (`spark.read.parquet("s3a://...")`) je isto kao sa HDFS-om.

### Redosled

1. **Zoomcamp, moduli za Docker, Spark i Kafku**, sa domaćim zadacima. Kad zapnem: hintovi umesto rešenja.
2. **Paralelno, svaki dan 2 do 3 zadatka sa window funkcijama** na DataLemur-u.
3. **Onda ASVSP projekat**, sa svim što sam naučio.
