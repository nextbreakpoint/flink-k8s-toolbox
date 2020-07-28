# How to setup a local environment   

Follow these instructions to setup a local environment for testing the Flink Operator.



## Install Docker and Kubernetes   

You can use Docker for Desktop or Minikube for this example.

### Docker for Desktop   

We assume you are using Docker for Desktop 2.2.0.0 or later with Kubernetes 1.15 or later.

Make sure that Docker is using at least 8Gb of memory (see Docker for Desktop settings).

### Minikube

We assume you are using Minikube 1.6.1 or later with Kubernetes 1.15 or later.

Make sure that Minikube is using at least 8Gb of memory:

    minikube start --memory=8gb ...

Don't forget to configure the environment before executing docker commands:

    eval $(minikube docker-env)



## Install Flink Operator    

Install kubectl:

    brew install kubectl

Install Helm:

    brew install helm

Create namespace:

    kubectl create namespace flink

Install operator's global resources:

    helm install flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd

Install operator's namespace resources:

    helm install --namespace flink flink-k8s-toolbox-operator helm/flink-k8s-toolbox-operator

Run Flink Operator:

    kubectl scale deployment -n flink flink-operator --replicas=1

Check that operator is running:

    kubectl get pod -n flink

Stop Flink Operator:

    kubectl scale deployment -n flink flink-operator --replicas=0

## Prepare Flink image    

Configure Docker environment (only for minikube):

    eval $(minikube docker-env)

Build custom Flink image:

    docker build -t example/flink:1.9.2 --build-arg flink_version=1.9.2 --build-arg scala_version=2.11 example/flink

## Prepare Flink job    

Configure Docker environment (only for minikube):

    eval $(minikube docker-env)

Build Flink jobs image:

    docker build -t example/flink-jobs:1 example/flink-jobs --build-arg repository=nextbreakpoint/flink-k8s-toolbox --build-arg version=1.3.5-beta

## Create Flink resources    

Create Flink ConfigMap resource:

    kubectl create -f example/config.yaml -n flink

Create Flink Secret resource:

    kubectl create -f example/secrets.yaml -n flink

Create Flink Cluster resource with standard storage class (required for Minikube):

    kubectl create -f example/cluster.yaml -n flink

Create Flink Cluster resource with hostpath storage class (required for Docker for Desktop):

    kubectl create -f example/cluster-hostpath.yaml -n flink

Get Flink Cluster resource:

    kubectl get fc test -o yaml -n flink

List Flink Cluster resources:

    kubectl get fc -n flink

Watch Flink Cluster resources:

    kubectl get fc -n flink --watch

## Check logs and remove pods     

Check pods are created:

    kubectl get pods -n flink --watch

Check logs of JobManager:

    kubectl logs -n flink flink-jobmanager-test-0 -c flink-jobmanager

Check logs of TaskManager:

    kubectl logs -n flink flink-taskmanager-test-0 -c flink-taskmanager

Force deletion of pods if Kubernetes get stuck:

    kubectl delete pod flink-jobmanager-test-0 --grace-period=0 --force -n flink
    kubectl delete pod flink-taskmanager-test-0 --grace-period=0 --force -n flink

## Patch Flink Cluster resource     

Example of patch operation to change pullPolicy:

    kubectl patch -n flink fc test --type=json -p '[{"op":"replace","path":"/spec/runtime/pullPolicy","value":"Always"}]'

Example of patch operation to change serviceMode:

    kubectl patch -n flink fc test --type=json -p '[{"op":"replace","path":"/spec/jobManager/serviceMode","value":"ClusterIP"}]'


## Optionally install a local Docker Registry

Create docker-registry files:

    ./docker-registry-setup.sh

Create docker-registry:

    kubectl create -f docker-registry.yaml

Add this entry to your hosts file (etc/hosts):

    127.0.0.1 registry

Create pull secrets in flink namespace:

    kubectl create secret docker-registry regcred -n flink --docker-server=registry:30000 --docker-username=test --docker-password=password --docker-email=<your-email>

Associate pull secrets to flink-operator service account:

    kubectl patch serviceaccount flink-operator -n flink -p '{"imagePullSecrets": [{"name": "regcred"}]}'

You can tag and push images to your local registry:

    docker tag flink:1.9.2 registry:30000/flink:1.9.2
    docker login registry:30000
    docker push registry:30000/flink:1.9.2



## Build Flink Operator from source code

Configure Docker environment (only for minikube):

    eval $(minikube docker-env)

Compile Docker image of Flink Operator:

    docker build -t flink-k8s-toolbox:1.3.5-beta .

Optionally tag and push Docker image to your local Docker registry:

    docker tag flink-k8s-toolbox:1.3.5-beta registry:30000/flink-k8s-toolbox:1.3.5-beta
    docker login registry:30000
    docker push registry:30000/flink-k8s-toolbox:1.3.5-beta

Run Flink Operator using Docker image:

    kubectl run flink-operator --restart=Never -n flink --image=registry:30000/flink-k8s-toolbox:1.3.5-beta --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always" } }' -- operator run --namespace=flink

Run Flink Operator using Helm and local registry:

    helm install --namespace flink flink-k8s-toolbox-operator helm/flink-k8s-toolbox-operator --set image.repository=registry:30000/flink-k8s-toolbox --set image.pullPolicy=Always

Run Flink Operator using Helm and local image:

    helm install --namespace flink flink-k8s-toolbox-operator helm/flink-k8s-toolbox-operator --set image.repository=flink-k8s-toolbox --set image.pullPolicy=Never
