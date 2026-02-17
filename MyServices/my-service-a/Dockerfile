#FROM openjdk:21-ea-1-jdk-slim AS builder
#
#WORKDIR /app
#
#COPY gradlew ./
#COPY gradle gradle/
#COPY build.gradle ./
#COPY settings.gradle ./
#COPY src src/
#
#RUN chmod +x ./gradlew
#RUN ./gradlew build --no-daemon

FROM openjdk:21-ea-1-jdk-slim

WORKDIR /app

#COPY --from=builder /app/build/libs/*.jar app.jar
EXPOSE 80

COPY build/libs/*.jar app.jar

ENTRYPOINT ["java", \
  "-Dotel.exporter.otlp.endpoint=http://otel-collector:4318", \
  "-jar", "app.jar"]