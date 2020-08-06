FROM gradle:6.6.1-jdk11 AS build
ADD . /src
WORKDIR /src
RUN gradle --no-daemon clean test shadowJar

FROM adoptopenjdk/openjdk11
COPY --from=build /src/build/libs/flink-k8s-toolbox-*-with-dependencies.jar /usr/local/bin/flink-k8s-toolbox.jar
RUN apt-get update -y && apt-get install -y curl
WORKDIR /
COPY entrypoint.sh .
RUN chmod u+x entrypoint.sh
HEALTHCHECK --retries=12 --interval=10s CMD curl -s localhost:8080/version || exit 1
EXPOSE 4444 8080
ENTRYPOINT ["sh", "/entrypoint.sh"]
