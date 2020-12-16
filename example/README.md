# Example of Flink deployment on Minikube   

Follow these instructions to set up a Minikube environment for testing the Flink Kubernetes Toolbox.

## Preparation   

Install Minikube 1.11.0 or later with Kubernetes 1.17.

Make sure that Minikube is using at least 8Gb of memory:

    minikube start --cpus=2 --memory=8gb --kubernetes-version v1.17.0

Install kubectl:

    brew install kubectl

Install Helm:

    brew install helm

## Install Flink Kubernetes Toolbox    

Create operator namespace:

    kubectl create namespace flink-operator

Create flink namespace:

    kubectl create namespace flink

Install resources:

    helm install flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd
    helm install flink-k8s-toolbox-roles helm/flink-k8s-toolbox-roles --namespace flink-operator --observedNamespace=flink
    helm install flink-k8s-toolbox-operator helm/flink-k8s-toolbox-operator --namespace flink-operator --observedNamespace=flink

Start the operator:

    kubectl scale deployment -n flink-operator flink-operator --replicas=1

## Prepare Flink image    

Configure Docker environment:

    eval $(minikube docker-env)

Build custom Flink image:

    docker build -t example/flink:1.9.2 --build-arg flink_version=1.9.2 --build-arg scala_version=2.11 example/flink

## Prepare Flink jobs    

Configure Docker environment:

    eval $(minikube docker-env)

Build Flink jobs image:

    docker build -t example/jobs:latest example/flink-jobs 

## Create Flink resources    

Create configmap resource:

    kubectl create -f example/flink-config.yaml -n flink

Create secrets resource:

    kubectl create -f example/flink-secrets.yaml -n flink

Create deployment resource:

    kubectl create -f example/deployment.yaml -n flink

Get deployment resource:

    kubectl get fd cluster-1 -o yaml -n flink

Watch cluster resources:

    kubectl get fc -n flink --watch

Watch job resources:

    kubectl get fj -n flink --watch

Check pods are created:

    kubectl get pods -n flink 

Check logs of JobManager:

    kubectl logs -n flink jobmanager-cluster-1-0 -c jobmanager

Check logs of TaskManager:

    kubectl logs -n flink taskmanager-cluster-1-0 -c taskmanager

## Patching a deployment resource     

Example of patch operation to change pullPolicy:

    kubectl patch -n flink fd cluster-1 --type=json -p '[{"op":"replace","path":"/spec/runtime/pullPolicy","value":"Always"}]'

Example of patch operation to change serviceMode:

    kubectl patch -n flink fd cluster-1 --type=json -p '[{"op":"replace","path":"/spec/jobManager/serviceMode","value":"ClusterIP"}]'

Example of patch operation to change savepointInterval:

    kubectl patch -n flink fd cluster-1 --type=json -p '[{"op":"replace","path":"/spec/jobs/0/spec/savepoint/savepointInterval","value":60}]'

