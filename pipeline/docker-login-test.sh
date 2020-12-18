#!/usr/bin/env sh

set -x
set -e

eval $(minikube docker-env)

echo -n $DOCKER_PASSWORD | docker login --username=$DOCKER_USERNAME --password-stdin
