ARG image

FROM maven:3.6.3-adoptopenjdk-11 AS build
ARG flink_version
ARG scala_version
COPY demo /demo
WORKDIR /demo
RUN mvn clean package -t toolchains.xml -DincludeFlinkRuntime=false -Dflink.version=${flink_version}

FROM ${image}
COPY --from=build /demo/target/demo-1.0.0-shaded.jar /flink-jobs.jar
