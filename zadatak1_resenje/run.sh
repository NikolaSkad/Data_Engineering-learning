#!/bin/bash
# Skripta za pokretanje resenja zadatka 1

# Brisanje starog kontejnera (radi ponovnog pokretanja skripte)
# Bez ovoga docker run puca sa "container name is already in use"
# Prvi put javlja da kontejner ne postoji, to je normalno i skripta nastavlja dalje
docker stop zad01-nginx
docker rm zad01-nginx

# Build pravi sliku iz Dockerfile-a u ovom folderu, -t je ime slike, tacka je folder sa Dockerfile-om
# Dockerfile kopira index.html u /usr/share/nginx/html, folder iz kog nginx servira stranice
docker build -t zad01-nginx-img .

# Run pokrece kontejner od NASE slike, ne od zvanicne nginx slike
# (cista nginx slika nema index.html i prikazuje podrazumevanu "Welcome to nginx!" stranicu)
# -d pokrece u pozadini, --name daje ime kontejneru, sve opcije idu pre imena slike
# -p 8080:80, levo port na Mac-u (localhost:8080), desno port u kontejneru (nginx slusa na 80)
docker run -d --name zad01-nginx -p 8080:80 zad01-nginx-img
