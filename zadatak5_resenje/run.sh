#!/bin/bash
# Skripta za pokretanje resenja zadatka 5 (aplikacija za glasanje, 5 kontejnera, bez Docker Compose)
# Pokrece se iz foldera zadatak5_resenje, jer se odavde vide vote/, result/ i worker/
# Tok glasa: vote -> redis -> worker -> db -> result

# 1. Brisanje starih kontejnera (radi ponovnog pokretanja skripte)
# Bez ovoga docker run puca sa "container name is already in use", jer dva kontejnera ne mogu imati isto ime
# Prvi put javlja da kontejneri ne postoje, to je normalno i skripta nastavlja dalje
# Brisu se SAMO kontejneri: slike se pregaze novim build-om, mreze se koriste postojece,
# a volumen se nikad ne brise jer su u njemu glasovi
docker stop zad05-redis zad05-db-postgres zad05-vote zad05-result zad05-worker
docker rm zad05-redis zad05-db-postgres zad05-vote zad05-result zad05-worker

# 2. Mreze
# back-tier: svih 5 servisa, preko nje se nalaze po imenu (na podrazumevanoj bridge mrezi to ne radi)
# front-tier: samo vote i result, servisi koje korisnik otvara u browseru
# Ako mreza vec postoji, create javi gresku "already exists", ne smeta, kontejneri se kace na postojecu
docker network create zad05-front-tier
docker network create zad05-back-tier

# 3. Volumen za bazu
# Ime mora biti isto kao u -v kod db kontejnera, inace Docker tiho napravi drugi, novi volumen
# Ako volumen vec postoji, create samo ispise ime i nista ne menja
docker volume create zad05-db-data

# 4. Build slika
# Build-uju se samo servisi koji imaju Dockerfile (vote, result, worker)
# redis i db se NE build-uju, koriste se zvanicne slike (redis:alpine, postgres:15-alpine)
# Putanja do foldera servisa ide na kraj umesto tacke, to je folder iz kog Docker uzima fajlove
# -f ne treba, jer svaki folder ima fajl koji se zove bas Dockerfile (u zad4 je trebao jer su bila dva sa drugim imenima)
docker build -t zad05-vote-img ./vote
docker build -t zad05-result-img ./result
# Worker slika (.NET 2.1) postoji samo za Intel (amd64), a Mac ima Apple cip (arm64), pa mora --platform
docker build --platform linux/amd64 -t zad05-worker-img ./worker

# 5. Pokretanje kontejnera
# Sve opcije idu PRE imena slike, slika je uvek poslednja
# -d pokrece u pozadini, nema --rm jer su ovo servisi koji stalno rade
# (ako neki pukne, kontejner ostaje pa mu se mogu videti logovi)

# Redis: red u koji vote ubacuje glasove
# --network-alias redis jer vote i worker traze host "redis", a kontejner se zove zad05-redis
# Alias je dodatno ime koje vazi samo u toj mrezi, pravo ime kontejnera ostaje isto
# U zad05-back-tier i "zad05-redis" i "redis" vode do istog kontejnera,
# a van mreze (docker ps, logs, stop) postoji samo zad05-redis
# Alias ne treba redis-u, nego onima koji ga traze (vote i worker imaju "redis" upisano u kodu)
# Da se kontejner zove samo --name redis, alias ne bi trebao (ime kontejnera vec vazi u mrezi)
# Mreza i alias se daju odmah u run, inace kontejner zavrsi na bridge mrezi koja mu ne treba
docker run -d --name zad05-redis --network zad05-back-tier --network-alias redis -p 6379:6379 redis:alpine

# Db (PostgreSQL): baza u koju worker upisuje glasove, a result ih cita
# --network-alias db jer worker i result traze host "db" (sa --name db alias ne bi trebao)
# -v ime_volumena:putanja_u_kontejneru, levo IME (volumen), desno gde postgres cuva podatke
# Putanja se nalazi sa: docker image inspect postgres:15-alpine --format '{{json .Config.Volumes}}'
# -e POSTGRES_HOST_AUTH_METHOD=trust jer se worker i result povezuju BEZ lozinke,
# a bez lozinke ili trust-a postgres slika nece ni da se pokrene
# Port se ne mapira, bazi se pristupa samo iznutra preko back-tier mreze
docker run -d --name zad05-db-postgres --network zad05-back-tier --network-alias db -v zad05-db-data:/var/lib/postgresql/data -e POSTGRES_HOST_AUTH_METHOD=trust postgres:15-alpine

# Vote: stranica za glasanje
# -p 5001:80, levo port na Mac-u, desno port u kontejneru (gunicorn slusa na 80)
# Bind mount: folder vote/ sa Mac-a se vidi u kontejneru kao /app (nista se ne kopira, isti folder sa dva mesta)
# --mount trazi imenovane delove (type, src, dst), oblik "$PWD/vote":/app je samo za -v
# Za razliku od -v, --mount javi gresku ako folder ne postoji, umesto da tiho napravi prazan
# "$PWD/vote" mora biti puna putanja (samo vote bi Docker shvatio kao ime volumena)
# i u navodnicima jer putanja ima razmak (DE Praksa)
docker run -d --name zad05-vote -p 5001:80 --network zad05-front-tier --mount type=bind,src="$PWD/vote",dst=/app zad05-vote-img
# Vote mora i na back-tier, jer tamo je redis u koji salje glasove
# Run prima jednu mrezu, druga se kaci naknadno (kao network connect u zad3)
docker network connect zad05-back-tier zad05-vote

# Result: prikaz rezultata
# -p 5050:80 umesto 5000:80 iz zadatka, jer port 5000 na Mac-u drzi AirPlay Receiver
# ("address already in use"); sa iskljucenim AirPlay-om moze nazad na 5000
# -p 5858:5858 je port za Node debugger, Docker ne otvara nijedan port sam (ni EXPOSE ga ne otvara),
# zato i kad su brojevi isti mora -p
# Bind mount isto kao kod vote, ali folder result/
docker run -d --name zad05-result -p 5050:80 -p 5858:5858 --network zad05-front-tier --mount type=bind,src="$PWD/result",dst=/app zad05-result-img
# Result mora i na back-tier, jer tamo je db iz koje cita glasove
docker network connect zad05-back-tier zad05-result

# Worker: prebacuje glasove iz redis-a u db
# Samo back-tier, nema port jer mu niko ne pristupa spolja
# --platform linux/amd64 isto kao pri build-u, bez njega radi ali ispise upozorenje
# Pokrece se poslednji, kad redis i db vec rade
docker run -d --name zad05-worker --platform linux/amd64 --network zad05-back-tier zad05-worker-img

# SVE KOMANDE - bez komentara

# docker stop zad05-redis zad05-db-postgres zad05-vote zad05-result zad05-worker
# docker rm zad05-redis zad05-db-postgres zad05-vote zad05-result zad05-worker

# docker network create zad05-front-tier
# docker network create zad05-back-tier

# docker volume create zad05-db-data

# docker build -t zad05-vote-img ./vote
# docker build -t zad05-result-img ./result
# docker build --platform linux/amd64 -t zad05-worker-img ./worker

# docker run -d --name zad05-redis --network zad05-back-tier --network-alias redis -p 6379:6379 redis:alpine
# docker run -d --name zad05-db-postgres --network zad05-back-tier --network-alias db -v zad05-db-data:/var/lib/postgresql/data -e POSTGRES_HOST_AUTH_METHOD=trust postgres:15-alpine
# docker run -d --name zad05-vote -p 5001:80 --network zad05-front-tier --mount type=bind,src="$PWD/vote",dst=/app zad05-vote-img
# docker run -d --name zad05-result -p 5050:80 -p 5858:5858 --network zad05-front-tier --mount type=bind,src="$PWD/result",dst=/app zad05-result-img
# docker run -d --name zad05-worker --platform linux/amd64 --network zad05-back-tier zad05-worker-img

# docker network connect zad05-back-tier zad05-vote
# docker network connect zad05-back-tier zad05-result
