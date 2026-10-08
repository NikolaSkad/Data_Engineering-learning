FROM node:10 AS build
WORKDIR /app
COPY package*.json .
RUN npm install
COPY . .
RUN npx ng build

FROM nginx:alpine
COPY --from=build /app/dist/angular-zadaci /usr/share/nginx/html
EXPOSE 80
