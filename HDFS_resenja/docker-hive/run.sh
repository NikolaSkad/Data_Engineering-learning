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


# ===== Hive - Zadatak 1 =====

# --- bash u hive-server kontejneru (docker exec -it hive-server bash) ---

# 2. Ručno napraviti data.csv (bez zaglavlja, bez razmaka oko zareza)
cd /opt
cat > data.csv <<'CSV'
1,Marko,28,M
2,Ana,35,F
3,Nikola,42,M
4,Jelena,24,F
5,Stefan,31,M
6,Milica,29,F
CSV
cat -A data.csv

# 3. Kopirati data.csv na HDFS
hdfs dfs -mkdir -p /user/emp
hdfs dfs -put -f data.csv /user/emp
hdfs dfs -ls /user/emp

# --- beeline (beeline -u jdbc:hive2://localhost:10000) ---

# 1. Kreirati tabelu Emp (razdvajač ',' mora da odgovara CSV fajlu, inače su sve vrednosti NULL)
DROP TABLE IF EXISTS Emp;
CREATE TABLE Emp (id INT, name STRING, age INT, gender STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

# 4. Učitati data.csv sa HDFS-a u tabelu (bez LOCAL; fajl se PREMEŠTA iz /user/emp u tabelu)
LOAD DATA INPATH '/user/emp/data.csv' INTO TABLE Emp;

# 5. Proveriti da su podaci učitani
SELECT * FROM Emp;

# 6. Zaposlene starije od 30 izvesti u CSV sa delimiterom ';' na HDFS
INSERT OVERWRITE DIRECTORY '/user/emp_stariji' ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' SELECT * FROM Emp WHERE age > 30;

# (provera) izađi iz beeline-a i pogledaj izlaz na HDFS-u
!quit
hdfs dfs -ls /user/emp_stariji
hdfs dfs -cat /user/emp_stariji/*
