#!/usr/bin/env sh

set -x
set -e

wget https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
chmod +x minikube-linux-amd64
sudo mv minikube-linux-amd64 /usr/local/bin/minikube
sudo minikube start --memory=6gb --vm-driver=none --kubernetes-version v1.17.0
sudo chown -R $USER $HOME/.kube $HOME/.minikube
sudo chgrp -R $USER $HOME/.kube $HOME/.minikube

