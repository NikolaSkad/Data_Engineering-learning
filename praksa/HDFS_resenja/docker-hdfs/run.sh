# ===== Na hostu (Mac) =====

# ugasi klaster i obriši volume-e (čist start)
docker compose down -v
# podigni namenode + 3 datanode-a u pozadini
docker compose up -d
# uđi u namenode kontejner (komande ispod se kucaju ručno u kontejneru)
docker exec -it namenode bash


# ===== Unutar namenode kontejnera =====

# pređi u home direktorijum (/root)
cd

# 1. Napraviti direktorijum /test na HDFS
hdfs dfs -ls /
hdfs dfs -mkdir /test

# 2. Proveriti da je direktorijum kreiran
hdfs dfs -ls /

# 3. Kreirati neprazan tekstualni fajl u lokalu
echo "neki text u ovom fajlu" > test_text.txt
cat test_text.txt

# 4. Kopirati fajl na HDFS u /test
hdfs dfs -put test_text.txt /test/
hdfs dfs -ls /test/

# 5. Prikazati sadržaj fajla
hdfs dfs -cat /test/test_text.txt

# (dodatno) Kopirati fajl unutar HDFS-a u /test/copy
hdfs dfs -mkdir /test/copy
hdfs dfs -cp /test/test_text.txt /test/copy/copy_test_text.txt
hdfs dfs -ls /test/copy/

# 6. Premestiti fajl u /test/new (folder mora prvo da postoji)
hdfs dfs -mkdir /test/new
hdfs dfs -mv /test/test_text.txt /test/new
hdfs dfs -ls /test
hdfs dfs -ls /test/new

# 7. Preimenovati fajl u new.txt (mv služi i za preimenovanje)
hdfs dfs -mv /test/new/test_text.txt /test/new/new.txt
hdfs dfs -ls /test/new

# 8. Preuzeti new.txt sa HDFS-a u lokalni direktorijum (/root)
hdfs dfs -get /test/new/new.txt
pwd
ls -l /root
cat new.txt

# 9. Prikazati veličinu svih fajlova u /test
# (prva kolona = veličina, druga = zauzeće sa replikama)
hdfs dfs -du -h /test

# 10. Obrisati kompletan /test direktorijum
hdfs dfs -rm -r -f /test

# provera da /test više ne postoji
hdfs dfs -ls /
