#!/usr/bin/env sh

set -x
set -e

export OPERATOR_HOST=$(minikube ip)

docker tag -t integration/flink:latest integration/flink:1.12.1
export FLINK_VERSION=1.12.1
export SCALA_VERSION=2.12
./gradlew integrationTest --info

docker tag -t integration/flink:latest integration/flink:1.11.3
export FLINK_VERSION=1.11.3
export SCALA_VERSION=2.12
./gradlew integrationTest --info
