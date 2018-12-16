FROM maven:3.6.0-jdk-11 as build
COPY pom.xml pom.xml
COPY src src
RUN mvn install -q

FROM azul/zulu-openjdk-alpine:11.0.1
COPY --from=build /target/log-telemetry.jar /log-telemetry.jar
ENTRYPOINT java -server -XX:+UseNUMA -jar /log-telemetry.jar

#NOTE: you should replace 'latest' with the specific version you want
#docker build -t log-telemetry:latest .
#docker history
#docker run -d --name log-telemetry-instance -p 8098:8098 log-telemetry:latest