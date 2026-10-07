# Inicijalizacija osnovne slike (base image)
FROM node:22-alpine

# Radni direktorijum gde ce kod aplikacije biti smesten unutar kontejnera
WORKDIR /usr/src/app

# Kopiranje package fajlova pre koda kako bi se npm ci pokrenuo
# samo kada se promeni package.json ili package-lock.json
COPY package*.json ./
RUN npm ci --omit=dev

# Kopiranje ostatka koda - ovaj i svi sledeci slojevi se ponovo izvrsavaju kada se kod promeni
COPY . .

# Sigurnosna praksa - node image vec ima "node" korisnika, ne pokrecemo app kao root
USER node

# Dokumentacija da aplikacija slusa na portu 8080 (ne otvara stvarno port)
EXPOSE 8080

# Komanda koja se pokrece kada kontejner startuje
CMD ["node", "server.js"]

# Pravi Docker image od Dockerfile-a i taguje ga kao moja_app:v1.0
# docker build -t moja_app:v1.0 .

# Pokrecece kontejner u pozadini (-d), ime kontejnera je moj_kontejner,
# mapira port 8080 sa masine na port 8080 unutar kontejnera
# docker run -d --name moj_kontejner -p 8080:8080 moja_app:v1.0