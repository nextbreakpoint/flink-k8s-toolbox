ARG repository
ARG version

FROM busybox AS build
RUN wget -O flink-jobs.jar https://github.com/nextbreakpoint/flink-workshop/releases/download/v1.2.3/com.nextbreakpoint.flinkworkshop-1.2.3.jar

FROM ${repository}:${version}
COPY --from=build /flink-jobs.jar /flink-jobs.jar
