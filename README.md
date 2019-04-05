# flink-submit

FlinkSubmit is a simple CLI utility for managing Flink clusters on Kubernetes. It requires a pod running in Kubernetes and a service account. 

## How to build

Build the application using Maven:

    mvn clean package

Maven creates a fat jar and a Docker image.

### Push image to registry

Create a tag and push the image to a registry:

    docker tag flink-submit:1.0.0 some-registry/flink-submit:1.0.0
    
    docker login some-registry
    
    docker push some-registry/flink-submit:1.0.0

## How to use

List available commands with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar --help

The command will produce the output:

    Usage: FlinkSubmit [OPTIONS] COMMAND [ARGS]...

    Options:
      -h, --help  Show this message and exit

    Commands:
      create  Create a cluster
      delete  Delete a cluster
      submit  Submit a job
      cancel  Cancel a job
      list    List jobs

### How to create a cluster

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        create \
        --kube-config=some-kubectl.conf \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --image=docker-repo/image-name:image-version \
        --image-pull-secrets=secrets-name \   
        --jobmanager-service-mode=NodePort

Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar create --help

### How to delete a cluster

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        delete \
        --kube-config=some-kubectl.conf \
        --cluster-name=my-flink-cluster \
        --environment=test

Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar delete --help

### How to submit a job

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        submit \
        --kube-config=some-kubectl.conf \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --class-name=your-class \
        --jar-path=your-jar \
        --arguments="--input=...,--output=..."

Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar submit --help

### How to cancel a job

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        cancel \
        --kube-config=some-kubectl.conf \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --create-savepoint \
        --job-id=your-job-id

Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar cancel --help

### How to list jobs

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        list \
        --kube-config=some-kubectl.conf \
        --cluster-name=my-flink-cluster \
        --environment=test

Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar list --help

## Create service account and pod

Create service account and RBAC role:

    kubectl create -f flink-submit-rbac.yaml
    
Check that service account has been created:

    kubectl get serviceaccounts flink-submit -o yaml     
    
Create flink-submit pod:

    kubectl run flink-submit --restart=Never --image=nextbreakpoint/flink-submit:1.0.0 --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-submit" } }, "spec": { "serviceAccountName": "flink-submit", "imagePullPolicy": "Always", "imagePullSecrets": [{"name": "your-pull-secrets"}] } }'
    
Check that pod has been created:

    kubectl get pod flink-submit -o yaml     

Make sure that the pod has a label app with value flink-submit and it is running with the service account. 
    
Get logs of flink-submit:
    
    kubectl logs flink-submit
    
### How to run as server

Run as server within Kubernetes:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar server
    
Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar server --help
