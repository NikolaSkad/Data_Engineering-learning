# for script 06

# master is set in code (.master("spark://localhost:7077"))
python 06_spark_sql.py \
    --input_green=data/pq/green/2020/ \
    --input_yellow=data/pq/yellow/2020/ \
    --output=data/report-2020

# master is passed via spark-submit (no .master() in code)
URL="spark://localhost:7077"

spark-submit \
    --master="${URL}" \
    06_spark_sql.py \
        --input_green=data/pq/green/2021/ \
        --input_yellow=data/pq/yellow/2021/ \
        --output=data/report-2021