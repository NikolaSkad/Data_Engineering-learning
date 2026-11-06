#!/bin/bash
# Skripta za pokretanje resenja zadatka 3

# Brisanje starih kontejnera (radi ponovnog pokretanja skripte)
# Prvo stop pa rm, jer rm -f ubija Redis pre nego sto snimi podatke u volumen
docker stop zad03-pyhton-kontinjer zad03-redis-kontinjer
docker rm zad03-pyhton-kontinjer zad03-redis-kontinjer

# Mreza preko koje se kontejneri nalaze po imenu (na podrazumevanoj mrezi to ne radi)
docker network create zad03-webnet

# Build pravi sliku iz Dockerfile-a, -t je ime slike, tacka je folder sa Dockerfile-om
# Build ne prima --name, -d, -p ni --network, to ide u run
docker build -t zad03-python-img .

# Run pravi kontejner od slike, sve opcije idu pre imena slike
# Python kontejner se namerno pravi bez mreze
docker run -d --name zad03-pyhton-kontinjer -p 8080:80 zad03-python-img

# Kacenje Python kontejnera na mrezu nakon kreiranja (mreza moze naknadno, port i volumen ne mogu)
docker network connect zad03-webnet zad03-pyhton-kontinjer

# Redis se kaci na mrezu odmah pri kreiranju
# --network-alias redis-zad03 jer app.py trazi host="redis-zad03", a kontejner se zove drugacije
# -v redis_data_zad03:/data cuva Redis podatke van kontejnera, posle -v mora odmah vrednost
# redis:8.10.2 je zvanicna slika, ne treba build, a lokalni tag ne bi radio na drugom racunaru
docker run -d --network zad03-webnet --network-alias redis-zad03 -v redis_data_zad03:/data --name zad03-redis-kontinjer -p 6379:6379 redis:8.10.2