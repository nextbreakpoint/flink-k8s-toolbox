ARG repository
ARG version

FROM maven:3.6.3-adoptopenjdk-11 AS build
COPY demo /demo
WORKDIR /demo
RUN mvn clean package -t toolchains.xml -DincludeFlinkRuntime=false

FROM ${repository}:${version}
COPY --from=build /demo/target/demo-1.0.0-shaded.jar /flink-jobs.jar
