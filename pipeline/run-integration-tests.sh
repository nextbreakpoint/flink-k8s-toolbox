#!/usr/bin/env sh

set -x
set -e

export OPERATOR_HOST=$(minikube ip)

docker tag -t integration/flink:latest integration/flink:${FLINK_VERSION}
./gradlew integrationTest --info

