ARG repository
ARG version

FROM ${repository}:${version} AS build
RUN apt-get update -y && apt-get install -y wget
RUN wget -O flink-jobs.jar https://github.com/nextbreakpoint/flink-workshop/releases/download/v1.2.1/com.nextbreakpoint.flinkworkshop-1.2.1.jar

FROM ${repository}:${version}
COPY --from=build /flink-jobs.jar /flink-jobs.jar
