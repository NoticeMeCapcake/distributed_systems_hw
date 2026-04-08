FROM eclipse-temurin:21-jdk-alpine
WORKDIR /app
COPY build/libs/node-1.0.0.jar app.jar
ENV JAVA_OPTS="-Xms128m -Xmx256m"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]