#!/usr/bin/env sh

wget https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
chmod +x minikube-linux-amd64
sudo mv minikube-linux-amd64 /usr/local/bin/minikube
sudo minikube start --memory=8gb --vm-driver=none
sudo chown -R $USER $HOME/.kube $HOME/.minikube
sudo chgrp -R $USER $HOME/.kube $HOME/.minikube
