# How to setup a local environment   

Follow these instructions to setup a local environment for testing the Flink Operator.



## Configure Docker for Desktop   

We assume you are using Docker for Desktop 2.0.0.3 or later and you have enabled Kubernetes.

**Please verify that Docker is using at least 8Gb of memory (see Docker for Desktop settings).**

### Install Flink Operator    

Install kubectl:

    brew install kubectl

Install Helm:

    brew install kubernetes-helm

Install Tiller:

    helm init --history-max 200 --kube-context docker-for-desktop

Create namespace:

    kubectl create namespace flink

Install operator's global resources:

    helm install --name flink-k8s-toolbox-global helm/flink-k8s-toolbox-global

Install operator's namespace resources:

    helm install --name flink-k8s-toolbox-services --namespace flink helm/flink-k8s-toolbox-services

Run Flink Operator:

     kubectl scale deployment -n flink flink-operator --replicas=1

Check that operator is running:

     kubectl get pod -n flink 

Stop Flink Operator:

     kubectl scale deployment -n flink flink-operator --replicas=0

### Prepare Flink image    

Build custom Flink image:

    docker build -t flink:1.7.2 --build-arg flink_version=1.7.2 --build-arg scala_version=2.11 example/flink

### Prepare Flink job    

Checkout sample Flink job:

    git clone https://github.com/nextbreakpoint/flink-workshop.git

Change directory:

    cd flink-workshop/flink/com.nextbreakpoint.flinkworkshop

Tag Flink image:

    docker tag flink:1.7.2 workshop-flink:1.7.2

Build Flink JAR file:

    mvn clean package

Create empty directory:

    mkdir ~/flink-jobs

Copy JAR file to empty directory:

    cp target/com.nextbreakpoint.flinkworkshop-1.0.1.jar ~/flink-jobs

Change directory:

    cd ~/flink-jobs

Create Docker file:

    cat <<EOF >Dockerfile
    FROM flink-k8s-toolbox:1.1.9-beta
    COPY com.nextbreakpoint.flinkworkshop-1.0.1.jar /flink-jobs.jar
    EOF

Create Flink job Docker image:

    docker build -t flink-jobs:1 .

### Create Flink resources    

Create Flink ConfigMap resource:

    kubectl create -f example/config-map.yaml -n flink

Create Flink Secret resource:

    kubectl create -f example/secrets.yaml -n flink

Create Flink Cluster resource:

    kubectl create -f example/flink-cluster-test.yaml -n flink

Get Flink Cluster resource:

    kubectl get fc -o yaml -n flink

### Check logs and remove pods     

Check pods are created:

    kubectl get pods -n flink --watch

Check logs of JobManager:

    kubectl logs -n flink flink-jobmanager-test-0

Check logs of TaskManager:

    kubectl logs -n flink flink-taskmanager-test-0

Force deletion of pods if Kubernetes get stuck:

    kubectl delete pod flink-jobmanager-test-11-0 --grace-period=0 --force -n flink
    kubectl delete pod flink-taskmanager-test-11-0 --grace-period=0 --force -n flink

### Patch Flink Cluster resource     

Example of patch operation to trigger cluster restart:

    kubectl patch -n flink fc test --type=json -p '[{"op":"replace","path":"/spec/flinkImage/pullPolicy","value":"Always"}]'



## Optionally install a local Docker Registry

Create docker-registry files:

    pushd kube
    ./docker-registry-setup.sh
    popd

Create docker-registry:

    kubectl create -f ./example/docker-registry.yaml

Add this entry to your hosts file (etc/hosts):

    127.0.0.1 registry

Create pull secrets in flink namespace:

    kubectl create secret docker-registry regcred -n flink --docker-server=registry:30000 --docker-username=test --docker-password=password --docker-email=<your-email>

Associate pull secrets to flink-operator service account:

    kubectl patch serviceaccount flink-operator -n flink -p '{"imagePullSecrets": [{"name": "regcred"}]}'

You can tag and push images to your local registry:

    docker tag flink:1.7.2 registry:30000/flink:1.7.2
    docker login registry:30000
    docker push registry:30000/flink:1.7.2



## Build Flink Operator from source code

Compile Docker image of Flink Operator:

    docker build -t flink-k8s-toolbox:1.1.9-beta .

Optionally tag and push Docker image to your local Docker registry:

    docker tag flink-k8s-toolbox:1.1.9-beta registry:30000/flink-k8s-toolbox:1.1.9-beta
    docker login registry:30000
    docker push registry:30000/flink-k8s-toolbox:1.1.9-beta

Run Flink Operator using Docker image:

    kubectl run flink-operator --restart=Never -n flink --image=registry:30000/flink-k8s-toolbox:1.1.9-beta --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always" } }' -- operator run --namespace=flink

Run Flink Operator using Helm and local registry:

    helm install --name flink-k8s-toolbox-services --namespace flink helm/flink-k8s-toolbox-services --set image.repository=registry:30000/flink-k8s-toolbox --set image.pullPolicy=Always

Run Flink Operator using Helm and local image:

    helm install --name flink-k8s-toolbox-services --namespace flink helm/flink-k8s-toolbox-services --set image.repository=flink-k8s-toolbox --set image.pullPolicy=Never 

## Use Minikube instead of Docker for Desktop

All the previous commands should apply to Minikube.

**Please ensure that Minikube is using at least 8Gb of memory:**

    minikube start --memory=8gb ...
