docker compose down -v
docker compose up -d
docker exec -it namenode bash
cd

hdfs dfs -ls /
hdfs dfs -mkdir /test
hdfs dfs -ls /
echo "neki text u ovom fajlu" > test_text.txt
cat test_text.txt

dfs dfs -put test_text.txt /test/
dfs dfs -ls /test/

hdfs dfs -mkdir /test/copy
hdfs dfs -cp /test/test_text.txt /test/copy/copy_test_text.txt
hdfs dfs -ls /test/copy/

hdfs dfs -mkdir /test/new
hdfs dfs -mv /test/test_text.txt /test/new
dfs dfs -ls /
dfs dfs -ls /test/new

hdfs dfs -mv /test/new/test_text.txt /test/new/new.txt
hdfs dfs -ls /test/new

hdfs dfs -get /test/new/new.txt
pwd
ls -l /root
cat new.txt

hdfs dfs -du -h /test

dfs dfs -rm -r -f /test

dfs dfs -ls /