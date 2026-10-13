# when the docker is up then docker
# not run the whole script paste in terminal line by line
# cus this docker exec -it hive-server bash can't continiue runing script, so lines under wouldn't execute
# ===== Hive - Primer 1 =====
# Zakačiti se na hive-server docker kontejner
docker exec -it hive-server bash
# Pokrenuti hive klijent - beeline
beeline -u jdbc:hive2://localhost:10000
# Kreirati tabelu pokes
CREATE TABLE pokes (foo INT, bar STRING);
# Uvesti podatke u tabelu iz lokalnog fajla
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;
# Pokrenuti neki upit
SELECT count(*) FROM pokes;

# ===== Hive - Primer 2 =====
# (i dalje u beeline-u, nastavak na Primer 1)

# Kreirati particionisanu tabelu invites (ds = kolona po kojoj se dele particije)
CREATE TABLE invites (foo INT, bar STRING) PARTITIONED BY (ds STRING);
# Prikazati strukturu tabele (ds se vidi kao partition column)
DESCRIBE invites;
# Učitati kv2.txt u particiju ds='2008-08-15'
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv2.txt' OVERWRITE INTO TABLE invites PARTITION (ds='2008-08-15');
# Učitati kv3.txt u particiju ds='2008-08-08'
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv3.txt' OVERWRITE INTO TABLE invites PARTITION (ds='2008-08-08');
# Upit nad samo jednom particijom
SELECT a.foo FROM invites a WHERE a.ds='2008-08-15';
# Izvesti rezultat upita u HDFS direktorijum /tmp/hdfs_out (CSV, razdvojeno zarezom)
INSERT OVERWRITE DIRECTORY '/tmp/hdfs_out' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT a.* FROM invites a WHERE a.ds='2008-08-15';

# (provera) izađi iz beeline-a i pogledaj izlaz na HDFS-u
!quit
hdfs dfs -ls /tmp/hdfs_out
hdfs dfs -cat /tmp/hdfs_out/*
