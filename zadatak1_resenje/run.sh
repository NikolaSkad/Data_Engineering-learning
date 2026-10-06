#!/bin/bash
# Skripta za pokretanje resenja zadatka 1

# Brisanje starog kontejnera (radi ponovnog pokretanja skripte)
# Bez ovoga docker run puca sa "container name is already in use"
# Prvi put javlja da kontejner ne postoji, to je normalno i skripta nastavlja dalje
docker stop zad01-nginx
docker rm zad01-nginx

# Run pravi kontejner od zvanicne nginx slike, sve opcije idu pre imena slike
# -d pokrece u pozadini, --name daje ime kontejneru
# -p 8080:80, levo port na Mac-u (localhost:8080), desno port u kontejneru (nginx slusa na 80)
# Zvanicna slika se ne build-uje, Docker je sam skine ako je nema lokalno
docker run -d --name zad01-nginx -p 8080:80 nginx:1.30.5
