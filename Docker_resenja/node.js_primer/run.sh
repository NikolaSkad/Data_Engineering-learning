#!/bin/bash
# Skripta za pokretanje node.js primera

# Brisanje starog kontejnera (radi ponovnog pokretanja skripte)
# Bez ovoga docker run puca sa "container name is already in use"
# Prvi put javlja da kontejner ne postoji, to je normalno i skripta nastavlja dalje
docker stop moj_kontejner
docker rm moj_kontejner

# Build pravi sliku iz Dockerfile-a u ovom folderu, -t je ime:tag slike, tacka je folder sa Dockerfile-om
# Lokalni node_modules ne ulazi u sliku (.dockerignore), paketi se instaliraju u slici sa npm ci
docker build -t moja_app:v1.0 .

# Run pokrece kontejner od nase slike, sve opcije idu pre imena slike
# -d pokrece u pozadini, --name daje ime kontejneru
# -p 8080:8080, levo port na Mac-u, desno port u kontejneru (server.js slusa na 8080, ne na 80)
docker run -d --name moj_kontejner -p 8080:8080 moja_app:v1.0
