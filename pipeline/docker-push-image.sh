#!/usr/bin/env sh

set -x
set -e

eval $(minikube docker-env)

docker tag integration/flinkctl:1.4.0-beta nextbreakpoint/flinkctl:1.4.0-beta
docker login --username=$DOCKER_USERNAME --password=$DOCKER_PASSWORD
docker push nextbreakpoint/flinkctl:1.4.0-beta
