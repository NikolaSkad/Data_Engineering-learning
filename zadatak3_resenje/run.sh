#!/bin/bash
# Skripta za pokretanje resenja zadatka 3

# Brisanje starih kontejnera (radi ponovnog pokretanja skripte)
docker stop zad03-pyhton-kontinjer zad03-redis-kontinjer
docker rm zad03-pyhton-kontinjer zad03-redis-kontinjer

docker network create zad03-webnet

docker build -t zad03-python-img .
docker run -d --name zad03-pyhton-kontinjer -p 8080:80 zad03-python-img
docker network connect zad03-webnet zad03-pyhton-kontinjer

docker run -d --network zad03-webnet --network-alias redis-zad03 -v redis_data_zad03:/data --name zad03-redis-kontinjer -p 6379:6379 redis:8.10.2