-- Primer - Movie ratings (MovieLens 100k)
-- Pre pokretanja (bash u hive-server kontejneru):
--   - python instaliran (python --version)
--   - /opt/weekday_mapper.py i /opt/movies.sql iskopirani sa docker cp
-- Pokretanje:
--   beeline -u jdbc:hive2://localhost:10000 -f /opt/movies.sql

-- 1. Tabela sa ocenama (u.data je razdvojen tabom)
DROP TABLE IF EXISTS u_data;
CREATE TABLE u_data (
  userid INT,
  movieid INT,
  rating INT,
  unixtime STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

-- 2. Učitati u.data iz lokalnog fajl sistema kontejnera
LOAD DATA LOCAL INPATH '/opt/u.data/u.data' OVERWRITE INTO TABLE u_data;

-- 3. Provera: treba 100000
SELECT COUNT(*) FROM u_data;

-- 4. Nova tabela gde je unixtime zamenjen danom u nedelji (1 = ponedeljak ... 7 = nedelja)
DROP TABLE IF EXISTS u_data_new;
CREATE TABLE u_data_new (
  userid INT,
  movieid INT,
  rating INT,
  weekday INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

-- 5. Registrovati python skriptu da bi Hive mogao da je pozove
add FILE /opt/weekday_mapper.py;

-- 6. Provući svaki red kroz python skriptu (TRANSFORM) i upisati rezultat u u_data_new
INSERT OVERWRITE TABLE u_data_new
SELECT
  TRANSFORM (userid, movieid, rating, unixtime)
  USING 'python weekday_mapper.py'
  AS (userid, movieid, rating, weekday)
FROM u_data;

-- 7. Broj ocena po danu u nedelji
SELECT weekday, COUNT(*)
FROM u_data_new
GROUP BY weekday;
