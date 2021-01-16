#!/usr/bin/env sh

set -x
set -e

eval $(minikube docker-env)

docker tag integration/jobs:${FLINK_VERSION} integration/jobs:latest
docker tag integration/flink:${FLINK_VERSION} integration/flink:latest
