#!/usr/bin/env sh

set -x
set -e

eval $(minikube docker-env)

docker tag integration/flinkctl:1.4.0-beta nextbreakpoint/flinkctl:$DOCKER_IMAGE_TAG
echo -n $DOCKER_PASSWORD | docker login --username=$DOCKER_USERNAME --password-stdin
docker push nextbreakpoint/flinkctl:$DOCKER_IMAGE_TAG
