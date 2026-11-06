FROM maven:3.8-openjdk-8 AS build
WORKDIR /app
COPY . .
RUN mvn package -DskipTests

FROM eclipse-temurin:8-jre
WORKDIR /app
COPY --from=build /app/target/web-shop-0.0.1-SNAPSHOT.jar app.jar
CMD ["java", "-jar", "app.jar"]