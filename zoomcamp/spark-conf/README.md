# Spark konfiguracija (lokalni pyspark iz `.venv`)

- `spark-env.sh`: `SPARK_LOCAL_HOSTNAME=localhost`, da Spark ne čeka ~30 s na DNS proveru imena Mac-a
- `log4j2.properties`: WARN/ERROR kao i inače, utišan je samo `NativeCodeLoader` (na Mac-u ne može da se reši)

Spark ove fajlove čita iz `$SPARK_HOME/conf`, pa je taj folder u `.venv`-u link na ovaj.
Posle novog `.venv`-a ili reinstalacije pyspark-a link se ponovo pravi iz korena repo-a:

```bash
PYS=.venv/lib/python3.14/site-packages/pyspark
rm -rf $PYS/conf && ln -s "$PWD/zoomcamp/spark-conf" $PYS/conf
```
