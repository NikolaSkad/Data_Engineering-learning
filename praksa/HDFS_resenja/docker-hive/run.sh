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


# ===== Hive - Zadatak 2 =====
# Hive komande su u zipcode.sql (u ovom folderu)

# --- Mac terminal (folder docker-hive) ---

# 1. Preuzeti CSV fajl (-L prati redirect, -o ime fajla)
curl -L -o zipcodes.csv https://raw.githubusercontent.com/spark-examples/spark-scala-examples/master/src/main/resources/free-zipcode-database.csv
# prebaciti CSV i SQL skriptu u hive-server kontejner (hdfs i beeline rade samo u kontejneru)
docker cp zipcodes.csv hive-server:/opt/zipcodes.csv
docker cp zipcode.sql hive-server:/opt/

# --- bash u hive-server kontejneru (docker exec -it hive-server bash) ---

cd /opt
ls -lh zipcodes.csv
head -3 zipcodes.csv

# 2. Kopirati fajl na HDFS
hdfs dfs -mkdir -p /user/zipcode
hdfs dfs -put -f zipcodes.csv /user/zipcode
hdfs dfs -ls -h /user/zipcode

# ----- A) sa skriptom zipcode.sql -----

# bash u kontejneru: izvrši sve komande iz zipcode.sql (koraci 3-6)
beeline -u jdbc:hive2://localhost:10000 -f /opt/zipcode.sql

# --- beeline (beeline -u jdbc:hive2://localhost:10000) - provere ---

SELECT COUNT(*) FROM Zipcode;     -- 81831
SELECT COUNT(*) FROM ZipcodeFL;   -- 3086
SELECT * FROM ZipcodeFL LIMIT 5;

# ----- B) ručno, komanda po komanda u beeline-u -----

# 3. Kreirati tabelu Zipcode
# OpenCSVSerde: skida navodnike i ne seče vrednosti sa zarezom ("Parc Parque, PR"); sve kolone čita kao STRING
# skip.header.line.count: preskače prvi red sa nazivima kolona
CREATE TABLE Zipcode (
  RecordNumber STRING, Zipcode STRING, ZipCodeType STRING, City STRING,
  State STRING, LocationType STRING, Lat STRING, Long STRING,
  Xaxis STRING, Yaxis STRING, Zaxis STRING, WorldRegion STRING,
  Country STRING, LocationText STRING, Location STRING, Decommisioned STRING,
  TaxReturnsFiled STRING, EstimatedPopulation STRING, TotalWages STRING, Notes STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES ('skip.header.line.count'='1');

# 4. Učitati podatke iz datoteke u tabelu (fajl se premešta iz /user/zipcode u tabelu)
LOAD DATA INPATH '/user/zipcode/zipcodes.csv' INTO TABLE Zipcode;
SELECT * FROM Zipcode LIMIT 5;
SELECT count(*) FROM Zipcode;   -- 81831

# 5. Prikazati sve podatke za kodove sa Floride
SELECT * FROM Zipcode WHERE State = 'FL';

# 6. Rezultate smestiti u novu tabelu ZipcodeFL (CTAS)
CREATE TABLE ZipcodeFL AS SELECT * FROM Zipcode WHERE State = 'FL';
SELECT COUNT(*) FROM ZipcodeFL;   -- 3086



# ===== Primer - Movie ratings (MovieLens 100k) =====
# Python je instaliran kroz Dockerfile (hive-server se pravi sa build: .)
# Fajlovi u ovom folderu: movies.sql (sve Hive komande), weekday_mapper.py (python skripta)

# --- Mac terminal (folder docker-hive) ---

# (ako je Dockerfile menjan) napraviti image i podići kontejnere
docker compose up -d --build

# 1. Preuzeti i raspakovati podatke (https + -L, inače se preuzme HTML redirect umesto zip-a)
curl -L -O https://files.grouplens.org/datasets/movielens/ml-100k.zip
unzip ml-100k.zip

# 2. Kopirati podatke i skripte u hive-server kontejner
# (posle svakog ponovnog pravljenja kontejnera ovo mora ponovo)
docker cp ml-100k hive-server:/opt/u.data
docker cp movies.sql hive-server:/opt/
docker cp weekday_mapper.py hive-server:/opt/

# --- bash u hive-server kontejneru (docker exec -it hive-server bash) ---

# 3. Provera da je sve na mestu
python --version
ls /opt/u.data/u.data /opt/movies.sql /opt/weekday_mapper.py

# 4. Hive deo - DVA NAČINA (izaberi jedan)
#
# NAČIN A - ceo fajl odjednom (bash u kontejneru):
#   pravi u_data, učitava podatke, TRANSFORM kroz python, GROUP BY weekday.
beeline -u jdbc:hive2://localhost:10000 -f /opt/movies.sql

# NAČIN B - ručno, komanda po komanda (beeline -u jdbc:hive2://localhost:10000):

# tabela sa ocenama (u.data je razdvojen tabom)
DROP TABLE IF EXISTS u_data;
CREATE TABLE u_data (userid INT, movieid INT, rating INT, unixtime STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
# učitati samo fajl u.data (ne ceo folder, inače uđu i u.item, u1.base...)
LOAD DATA LOCAL INPATH '/opt/u.data/u.data' OVERWRITE INTO TABLE u_data;
SELECT COUNT(*) FROM u_data;      -- 100000

# nova tabela gde je unixtime zamenjen danom u nedelji
DROP TABLE IF EXISTS u_data_new;
CREATE TABLE u_data_new (userid INT, movieid INT, rating INT, weekday INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
# registrovati python skriptu, pa svaki red provući kroz nju (TRANSFORM)
add FILE /opt/weekday_mapper.py;
INSERT OVERWRITE TABLE u_data_new SELECT TRANSFORM (userid, movieid, rating, unixtime) USING 'python weekday_mapper.py' AS (userid, movieid, rating, weekday) FROM u_data;

# --- provere (važe za oba načina) ---

# 5. Broj ocena (oba treba 100000)
SELECT COUNT(*) FROM u_data;
SELECT COUNT(*) FROM u_data_new;
# 6. weekday mora biti 1-7 (1 = ponedeljak, 7 = nedelja), ne unixtime
SELECT * FROM u_data_new LIMIT 5;
# 7. Broj ocena po danu u nedelji (7 redova, zbir 100000)
SELECT weekday, COUNT(*) FROM u_data_new GROUP BY weekday;
