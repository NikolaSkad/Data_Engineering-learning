#!/bin/bash
# Skripta za pokretanje resenja zadatka 2

# Brisanje starog kontejnera (radi ponovnog pokretanja skripte)
# Bez ovoga docker run puca sa "container name is already in use"
# Prvi put javlja da kontejner ne postoji, to je normalno i skripta nastavlja dalje
docker stop zad02-python
docker rm zad02-python

# Build pravi sliku iz Dockerfile-a u ovom folderu, -t je ime slike, tacka je folder sa Dockerfile-om
# Tek u ovoj slici su app.py, instaliran Flask i CMD ["python", "app.py"]
docker build -t zad02-python-img .

# Run pokrece kontejner od NASE slike, ne od python:3.12-slim
# (python:3.12-slim je prazan Python bez app.py, sa -d se odmah ugasi sa Exited (0))
# -p 8080:80, levo port na Mac-u, desno port u kontejneru (app.py slusa na 80)
docker run -d --name zad02-python -p 8080:80 zad02-python-img
