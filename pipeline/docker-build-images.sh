#!/usr/bin/env sh

set -x
set -e

eval $(minikube docker-env)

docker build -t integration/flinkctl:1.4.4-beta .
docker build -t integration/flink:1.11.3 integration/flink --build-arg flink_version=1.11.3 --build-arg scala_version=2.12
docker build -t integration/flink:1.12.1 integration/flink --build-arg flink_version=1.12.1 --build-arg scala_version=2.12
docker build -t integration/jobs:latest integration/jobs --build-arg repository=integration/flinkctl --build-arg version=1.4.4-beta
