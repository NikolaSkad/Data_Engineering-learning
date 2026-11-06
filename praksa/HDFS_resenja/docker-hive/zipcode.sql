-- Hive - Zadatak 2 (Zipcode)
-- Pre pokretanja:
--   - zipcodes.csv na HDFS-u u /user/zipcode (hdfs dfs -put -f zipcodes.csv /user/zipcode)
--   - /opt/zipcode.sql iskopiran sa docker cp
-- Pokretanje (bash u hive-server kontejneru):
--   beeline -u jdbc:hive2://localhost:10000 -f /opt/zipcode.sql

DROP TABLE IF EXISTS ZipcodeFL;
DROP TABLE IF EXISTS Zipcode;

-- 3. Kreirati tabelu Zipcode
-- OpenCSVSerde: skida navodnike i ne seče vrednosti sa zarezom ("Parc Parque, PR"); sve kolone čita kao STRING
-- skip.header.line.count: preskače prvi red sa nazivima kolona
CREATE TABLE Zipcode (
  RecordNumber STRING, Zipcode STRING, ZipCodeType STRING, City STRING,
  State STRING, LocationType STRING, Lat STRING, Long STRING,
  Xaxis STRING, Yaxis STRING, Zaxis STRING, WorldRegion STRING,
  Country STRING, LocationText STRING, Location STRING, Decommisioned STRING,
  TaxReturnsFiled STRING, EstimatedPopulation STRING, TotalWages STRING, Notes STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 4. Učitati podatke iz datoteke u tabelu (fajl se PREMEŠTA iz /user/zipcode u tabelu)
LOAD DATA INPATH '/user/zipcode/zipcodes.csv' OVERWRITE INTO TABLE Zipcode;
SELECT * FROM Zipcode LIMIT 5;
-- treba 81831
SELECT COUNT(*) FROM Zipcode;

-- 5. Prikazati sve podatke za kodove sa Floride
SELECT * FROM Zipcode WHERE State = 'FL';

-- 6. Rezultate smestiti u novu tabelu ZipcodeFL (CTAS = CREATE TABLE ... AS SELECT)
CREATE TABLE ZipcodeFL AS
SELECT * FROM Zipcode WHERE State = 'FL';
-- treba 3086
SELECT COUNT(*) FROM ZipcodeFL;
