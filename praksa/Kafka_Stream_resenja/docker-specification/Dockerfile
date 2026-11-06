# Build ide nativno (arm64 na Macu), jer .jar radi na svakoj platformi, a emulacija je spora
FROM --platform=$BUILDPLATFORM maven:3.9-eclipse-temurin-8 AS maven_build

# cache mount: Maven biblioteke ostaju između buildova, ne skidaju se svaki put ponovo
COPY ./producer /usr/producer
RUN --mount=type=cache,target=/root/.m2 mvn -q -f /usr/producer/pom.xml clean package

COPY ./primeri /usr/consumers
RUN --mount=type=cache,target=/root/.m2 mvn -q -f /usr/consumers/pom.xml clean package

# Runtime ostaje amd64 (platform u docker-compose.yaml), zbog RocksDB biblioteke u Kafka Streams
FROM eclipse-temurin:8-jre
COPY --from=maven_build /usr/producer/target /usr/producer
COPY --from=maven_build /usr/consumers/target /usr/consumers
ENTRYPOINT ["java","-cp","/usr/producer/kafka-topic-producer-0.0.1-SNAPSHOT.jar", "com.example.kafka_topic_producer.App"]
