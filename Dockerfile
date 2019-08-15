FROM gradle:5.5.1-jdk8 AS build
ADD . /src
WORKDIR /src
RUN gradle --no-daemon clean shadowJar

FROM openjdk:14-jdk-alpine
COPY --from=build /src/modules/cli/build/libs/flink-k8s-toolbox-*-with-dependencies.jar /flink-k8s-toolbox.jar
COPY entrypoint.sh /entrypoint.sh
RUN chmod u+x /entrypoint.sh
EXPOSE 4444 8080
ENTRYPOINT ["sh", "/entrypoint.sh"]
