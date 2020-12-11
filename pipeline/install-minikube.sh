#!/usr/bin/env sh

set -x
set -e

curl https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 -o /tmp/minikube-linux-amd64
chmod +x /tmp/minikube-linux-amd64
sudo mv /tmp/minikube-linux-amd64 /usr/local/bin/minikube

