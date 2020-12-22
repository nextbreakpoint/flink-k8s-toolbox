#!/usr/bin/env sh

set -x
set -e

eval $(minikube docker-env)

docker build -t integration/flinkctl:1.4.3-beta .
docker build -t integration/flink:1.9.2 integration/flink --build-arg flink_version=1.9.2 --build-arg scala_version=2.11
docker build -t integration/jobs:latest integration/jobs --build-arg repository=integration/flinkctl --build-arg version=1.4.3-beta
