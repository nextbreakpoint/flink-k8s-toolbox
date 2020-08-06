#!/usr/bin/env sh

set -x
set -e

eval $(minikube docker-env)

docker build -t integration/flink-k8s-toolbox:1.4.0-beta .
