#!/usr/bin/env sh

set -x
set -e

wget https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
chmod +x minikube-linux-amd64
sudo mv minikube-linux-amd64 /usr/local/bin/minikube
minikube start --cpus=2 --memory=6gb --vm-driver=docker --kubernetes-version v1.17.0
#chown -R $USER $HOME/.kube $HOME/.minikube
#chgrp -R $USER $HOME/.kube $HOME/.minikube

