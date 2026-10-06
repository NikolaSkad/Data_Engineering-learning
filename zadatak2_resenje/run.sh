docker stop zad02-python
docker rm zad02-python

docker build -t zad02-pyhton-img .
docker run -d --name zad02-python -p 8080:80 zad02-pyhton-img               