FROM maven:3.6.0-jdk-11 as build
COPY pom.xml pom.xml
COPY src src
RUN mvn install -q
RUN cat pom.xml | grep "^    <version>.*</version>$" | awk -F'[><]' '{print $3}' > version.txt

FROM azul/zulu-openjdk-alpine:11.0.1
COPY --from=build version.txt version.txt
RUN ver=$(<version.txt)
RUN rm version.txt

COPY --from=build /target/${artifactId}-${ver}.jar /${artifactId}-${ver}.jar
ENTRYPOINT java -server -XX:+UseNUMA -jar /${artifactId}-${ver}.jar

#NOTE: you should replace 'latest' with the specific version you want
#docker build -t ${artifactId}:latest .
#docker history
#docker run -d --name ${artifactId}-instance -p 8098:8098 ${artifactId}:latest