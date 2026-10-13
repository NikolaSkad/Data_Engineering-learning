# when the docker is up then docker
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