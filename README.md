# flink-submit

FlinkSubmit provides a solution for managing Flink clusters on Kubernetes. FlinkSubmit has three components: client cli, server api, sidecar controller:

- The client command-line interface provides the tools for managing clusters and jobs.

- The server api accepts requests from the cli and executes commands against Kubernetes and Flink.

- The sidecar controller is responsible of executing a job and monitoring its status.        

## How to build

Build the application using Maven:

    mvn clean package

Maven creates a fat jar and a Docker image.

### Push image to registry

Create a tag and push the image to a Docker registry:

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
      create   Create a cluster
      delete   Delete a cluster
      submit   Submit a job
      cancel   Cancel a job
      list     List jobs
      server   Run the server
      sidecar  Sidecar commands

## Create service account and install server api

Create service account and RBAC role:

    kubectl create -f flink-submit-rbac.yaml
    
Verify that service account has been created:

    kubectl get serviceaccounts flink-submit -o yaml     
    
Run flink-submit server api:

    kubectl run flink-submit --restart=Never --image=some-registry/flink-submit:1.0.0 --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-submit" } }, "spec": { "serviceAccountName": "flink-submit", "imagePullPolicy": "Always", "imagePullSecrets": [{"name": "your-pull-secrets"}] } }'
    
Verify that pod has been created:

    kubectl get pod flink-submit -o yaml     

The pod must have a label app with value flink-submit and it must run with flink-submit service account. 
    
Verify that there are no errors in the logs:
    
    kubectl logs flink-submit

Check the system events if the pod doesn't start:
    
    kubectl get events

## How to create a cluster

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        create \
        --cluster-name=test \
        --flink-image=nextbreakpoint/flink:1.7.2 \
        --sidecar-image=nextbreakpoint/flink-submit:1.0.0 \
        --image-pull-secrets=regcred \
        --sidecar-arguments="watch --cluster-name=test"

Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar create --help

## How to create a cluster and submit a job

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        create \
        --cluster-name=test \
        --flink-image=nextbreakpoint/flink:1.7.2 \
        --sidecar-image=nextbreakpoint/flink-submit:1.0.0 \
        --image-pull-secrets=regcred \
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/your-job-jar.jar \
        --sidecar-argument=--argument=--INPUT \
        --sidecar-argument=--argument=A \
        --sidecar-argument=--argument=--OUTPUT \
        --sidecar-argument=--argument=B 

## How to delete a cluster

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        delete \
        --cluster-name=my-flink-cluster \
        --environment=test

Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar delete --help

## How to submit a job

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        submit \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --class-name=your-class \
        --jar-path=your-jar 

### How to pass multiple job arguments:

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        submit \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --class-name=your-class \
        --jar-path=your-jar \
        --argument=--input \
        --argument=A \
        --argument=--output \
        --argument=B

Or execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        submit \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --class-name=your-class \
        --jar-path=your-jar \
        --arguments="--input A --output B"

## How to cancel a job

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        cancel \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --create-savepoint \
        --job-id=your-job-id

Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar cancel --help

## How to list the jobs

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar \
        list \
        --cluster-name=my-flink-cluster \
        --environment=test

Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar list --help

## More about server api and sidecar controller

The server api and the sidecar controller are usually managed by FlinkSubmit. 
However it might be necessary to run the server and the controller manually for testing.     
    
## How to run the server api

Run the server api within Kubernetes:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar server
    
Run the server api outside Kubernetes:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar server --port=4444 --kube-config=/your-kube-config.conf
    
Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar server --help

## How to run the sidecar controller

Run the sidecar controller within Kubernetes:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar sidecar watch --cluster-name=test
    
Run the sidecar controller outside Kubernetes:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar sidecar watch --cluster-name=test --kube-config=/your-kube-config.conf 
    
Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar sidecar watch --help

## How to submit a job from the sidecar controller

Run the sidecar controller within Kubernetes:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar sidecar submit --cluster-name=test
    
Run the sidecar controller outside Kubernetes:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar sidecar submit --cluster-name=test --kube-config=/your-kube-config.conf --class-name=your-main-class --jar-path=/your-job-jar.jar
    
Show more parameters with the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar sidecar submit --help
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/your-job-jar.jar \
        --sidecar-argument=--arguments="--input A --output B"
