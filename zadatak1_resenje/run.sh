docker stop zad01-nginx
docker rm zad01-nginx

docker run -d --name zad01-nginx -p 8080:80 nginx:1.30.5