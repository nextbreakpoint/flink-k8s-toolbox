#!/usr/bin/env sh

set -x
set -e

minikube start --cpus=2 --memory=6gb --vm-driver=docker --kubernetes-version v1.17.0
#chown -R $USER $HOME/.kube $HOME/.minikube
#chgrp -R $USER $HOME/.kube $HOME/.minikube

