docker build -t zad01-nginx-img .   
docker run -d --name zad01-nginx -p 8080:80 zad01-nginx-img  