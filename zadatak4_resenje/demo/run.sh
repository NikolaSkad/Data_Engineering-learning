#!/bin/bash
# Skripta za pokretanje resenja zadatka 4

# Brisanje starog run kontejnera (radi ponovnog pokretanja skripte)
# Build kontejner nema sta da se brise jer se pokrece sa --rm
docker stop zad04-run-kontejner
docker rm zad04-run-kontejner

# Volumen je deljeno skladiste preko kog build kontejner predaje .jar run kontejneru
# Ako volumen vec postoji, create samo ispise ime i nista ne menja
docker volume create zad04-jar-volume

# Dva Dockerfile-a u istom folderu, pa se sa -f bira koji se koristi
# Build slika ima Maven i JDK, run slika samo JRE (manja, dovoljna za java -jar)
docker build -f Dockerfile.build -t zad04-build-img .
docker build -f Dockerfile.run -t zad04-run-img .

# Build kontejner pri pokretanju radi mvn package i kopira .jar u /output (volumen)
# Namerno bez -d, skripta ceka da build zavrsi pre nego sto se pokrene run kontejner
# --rm brise kontejner kad zavrsi, a .jar ostaje u volumenu
docker run --rm --name zad04-build-kontejner -v zad04-jar-volume:/output zad04-build-img

# Run kontejner kaci isti volumen na /output i pokrece /output/app.jar
# -p 8080:8080 jer Spring Boot u kontejneru slusa na 8080 (sa 8080:80 stranica je prazna)
# :ro jer run kontejner .jar samo cita
docker run -d --name zad04-run-kontejner -p 8080:8080 -v zad04-jar-volume:/output:ro zad04-run-img
