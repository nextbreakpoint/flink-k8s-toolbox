#!/usr/bin/env sh

set -x
set -e

curl https://get.helm.sh/helm-v3.0.2-linux-amd64.tar.gz -o /tmp/helm.tar.gz
tar -xzf /tmp/helm.tar.gz
sudo cp linux-amd64/helm /usr/local/bin
rm -fR linux-amd64 helm.tar.gz
