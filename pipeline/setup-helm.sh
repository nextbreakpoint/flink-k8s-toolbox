#!/usr/bin/env sh

curl https://get.helm.sh/helm-v2.15.2-linux-amd64.tar.gz -o helm.tar.gz
mkdir helm
tar -xzf helm.tar.gz -C helm
sudo cp helm/linux-amd64/helm /usr/local/bin
sudo cp helm/linux-amd64/tiller /usr/local/bin
helm init
