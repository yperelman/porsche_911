# Multi-stage build: compile the fat jar with Maven, run it on a slim JRE.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
# Cache dependencies first (re-resolved only when pom.xml changes).
COPY pom.xml .
RUN mvn -q -B -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/stream-processor-*.jar app.jar
# Dashboard / REST port (matches server.port in application.yml).
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
