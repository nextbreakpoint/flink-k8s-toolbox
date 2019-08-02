#!/bin/sh

ROOT=$HOME/minikube

mkdir -p $ROOT/data
mkdir -p $ROOT/certs
mkdir -p $ROOT/auth
openssl req -config docker-registry-ssl.conf -extensions v3_ca -newkey rsa:4096 -nodes -sha256 -keyout $ROOT/certs/docker-registry.key -x509 -days 365 -subj "/CN=registry" -out $ROOT/certs/docker-registry.crt
openssl x509 -text -in $ROOT/certs/docker-registry.crt
docker run --rm --entrypoint htpasswd registry:2 -Bbn test password > $ROOT/auth/htpasswd

cat <<EOF >docker-registry.yaml
---
apiVersion: v1
kind: Pod
metadata:
  name: registry
  labels:
    app: registry
spec:
  containers:
  - name: registry
    image: registry:2
    imagePullPolicy: IfNotPresent
    ports:
      - containerPort: 30000
    volumeMounts:
      - mountPath: /var/lib/registry
        subPath: data
        name: registry-data
      - mountPath: /etc/certs
        subPath: certs
        name: registry-data
      - mountPath: /etc/auth
        subPath: auth
        name: registry-data
    env:
      - name: REGISTRY_HTTP_ADDR
        value: "0.0.0.0:30000"
      - name: REGISTRY_HTTP_TLS_CERTIFICATE
        value: "/etc/certs/docker-registry.crt"
      - name: REGISTRY_HTTP_TLS_KEY
        value: "/etc/certs/docker-registry.key"
      - name: REGISTRY_AUTH
        value: "htpasswd"
      - name: REGISTRY_AUTH_HTPASSWD_REALM
        value: "Registry Realm"
      - name: REGISTRY_AUTH_HTPASSWD_PATH
        value: "/etc/auth/htpasswd"
  volumes:
    - name: registry-data
      hostPath:
        path: $ROOT
---
apiVersion: v1
kind: Service
metadata:
  name: registry
  labels:
    app: registry
spec:
  selector:
    app: registry
  ports:
    - port: 30000
      targetPort: 30000
      nodePort: 30000
  type: NodePort
EOF
