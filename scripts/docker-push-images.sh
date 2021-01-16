#!/usr/bin/env sh

set -x
set -e

minikube addons enable registry

docker tag integration/flinkctl:1.4.4-beta $(minikube ip):5000/flinkctl:1.4.4-beta
docker tag integration/flink:1.9.2 $(minikube ip):5000/flink:1.9.2
docker tag integration/jobs:latest $(minikube ip):5000/jobs:latest

docker push $(minikube ip):5000/flinkctl:1.4.4-beta
docker push $(minikube ip):5000/flink:1.9.2
docker push $(minikube ip):5000/jobs:latest

eval $(minikube docker-env)

docker tag $(minikube ip):5000/flinkctl:1.4.4-beta integration/flinkctl:1.4.4-beta
docker tag $(minikube ip):5000/flink:1.9.2 integration/flink:1.9.2
docker tag $(minikube ip):5000/jobs:latest integration/jobs:latest
