#!/usr/bin/env sh

set -x
set -e

eval $(minikube docker-env)

docker tag integration/flinkctl:1.4.1-beta nextbreakpoint/flinkctl:$DOCKER_IMAGE_TAG
docker push nextbreakpoint/flinkctl:$DOCKER_IMAGE_TAG
