#!/usr/bin/env sh

set -x
set -e

eval $(minikube docker-env)

docker build -t integration/flink:1.9.2 example/flink --build-arg flink_version=1.9.2 --build-arg scala_version=2.11
docker build -t integration/flink-jobs:1 example/flink-jobs --build-arg repository=integration/flink-k8s-toolbox --build-arg version=1.3.4-beta
