ARG registry

FROM ${registry}flink-k8s-toolbox:1.2.2-beta AS build
RUN apt-get update -y && apt-get install -y wget
RUN wget -O flink-jobs.jar https://github.com/nextbreakpoint/flink-workshop/releases/download/v1.1.0/com.nextbreakpoint.flinkworkshop-1.1.0.jar

FROM ${registry}flink-k8s-toolbox:1.2.2-beta
COPY --from=build /flink-jobs.jar /flink-jobs.jar
