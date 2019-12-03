#!/usr/bin/env sh

curl https://get.helm.sh/helm-v2.15.2-linux-amd64.tar.gz -o helm.tar.gz
tar -xzf helm.tar.gz
sudo cp linux-amd64/helm /usr/local/bin
sudo cp linux-amd64/tiller /usr/local/bin
rm -fR linux-amd64 helm.tar.gz
helm init
