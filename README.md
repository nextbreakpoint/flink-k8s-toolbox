# flink-submit

This repository provides a set of tools for managing Flink clusters on Kubernetes, which includes a CLI, a server application, and a sidecar controller:

- The command-line interface provides the interface for managing clusters and jobs.

- The server application accepts requests from the cli and executes commands against Kubernetes and Flink.

- The sidecar controller is responsible of executing a job and monitoring its status.        

## License

The tools are distributed under the terms of BSD 3-Clause License.

    Copyright (c) 2019, Andrea Medeghini
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this
      list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.

    * Neither the name of the tools nor the names of its
      contributors may be used to endorse or promote products derived from
      this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
    FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
    DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
    SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

## Get Docker image

The Docker image can be downloaded from Docker Hub:

    docker fetch nextbreakpoint/flink-submit:1.0.0-alpha

Tag and push the image into your registry if required:

    docker tag nextbreakpoint/flink-submit:1.0.0-alpha some-registry/flink-submit:1.0.0-alpha

    docker login some-registry

    docker push some-registry/flink-submit:1.0.0-alpha

## Create service account and install server application

Create service account and RBAC role:

    kubectl create -f flink-submit-rbac.yaml

Verify that service account has been created:

    kubectl get serviceaccounts flink-submit -o yaml     

Run the server application using Docker Hub:

    kubectl run flink-submit --restart=Never --image=nextbreakpoint/flink-submit:1.0.0-alpha --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-submit" } }, "spec": { "serviceAccountName": "flink-submit", "imagePullPolicy": "Always" } }'

Or run the application using your registry:

    kubectl run flink-submit --restart=Never --image=some-registry/flink-submit:1.0.0-alpha --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-submit" } }, "spec": { "serviceAccountName": "flink-submit", "imagePullPolicy": "Always", "imagePullSecrets": [{"name": "your-pull-secrets"}] } }'

Verify that pod has been created:

    kubectl get pod flink-submit -o yaml     

The pod must have a label app with value *flink-submit* and it must run with *flink-submit* service account.

Verify that there are no errors in the logs:

    kubectl logs flink-submit

Check the system events if the pod doesn't start:

    kubectl get events

## Build from source code

Build the tools using Maven:

    mvn clean package

Maven will create a fat jar and a Docker image.

Create a tag and push the image to your Docker registry:

    docker tag flink-submit:1.0.0-alpha some-registry/flink-submit:1.0.0-alpha

    docker login some-registry

    docker push some-registry/flink-submit:1.0.0-alpha

## How to use the CLI

Execute the CLI using the Docker image or download the jar file and run the CLI with Java command.   

Show all commands using the jar file:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar --help

Or show all commands using the Docker image:

    docker run --rm -it nextbreakpoint/flink-submit:1.0.0-alpha --help

The output should look like:

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

### How to create a cluster

Execute the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar \
        create \
        --cluster-name=test \
        --flink-image=nextbreakpoint/flink:1.7.2-1 \
        --sidecar-image=nextbreakpoint/flink-submit:1.0.0-alpha \
        --image-pull-secrets=regcred \
        --sidecar-arguments="watch --cluster-name=test"

Show more parameters with the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar create --help

### How to create a cluster and submit a job

Create a Docker file:

    cat <<EOF >Dockerfile
    FROM nextbreakpoint/flink-submit:1.0.0-alpha
    COPY flink-jobs.jar /flink-jobs.jar
    EOF

Where flink-jobs.jar contains the code of your Flink jobs.

Create a Docker image:

    docker build -t flink-submit-with-jobs:1.0.0 .

Create a tag and push the image to your Docker registry:

    docker tag flink-submit-with-jobs:1.0.0 some-registry/flink-submit-with-jobs:1.0.0

    docker login some-registry

    docker push some-registry/flink-submit-with-jobs:1.0.0

Execute the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar \
        create \
        --cluster-name=test \
        --flink-image=nextbreakpoint/flink:1.7.2-1 \
        --sidecar-image=some-registry/flink-submit-with-jobs:1.0.0 \
        --image-pull-secrets=regcred \
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/flink-jobs.jar \
        --sidecar-argument=--argument=--INPUT \
        --sidecar-argument=--argument=A \
        --sidecar-argument=--argument=--OUTPUT \
        --sidecar-argument=--argument=B

### How to delete a cluster

Execute the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar \
        delete \
        --cluster-name=my-flink-cluster \
        --environment=test

Show more parameters with the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar delete --help

### How to run a job

Execute the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar \
        run \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --sidecar-image=some-registry/flink-submit-with-jobs:1.0.0 \
        --image-pull-secrets=regcred \
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/flink-jobs.jar

### How to pass multiple arguments to a job:

Execute the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar \
        run \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --sidecar-image=some-registry/flink-submit-with-jobs:1.0.0 \
        --image-pull-secrets=regcred \
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/flink-jobs.jar \
        --sidecar-argument=--argument=--INPUT \
        --sidecar-argument=--argument=A \
        --sidecar-argument=--argument=--OUTPUT \
        --sidecar-argument=--argument=B

Or execute the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar \
        run \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/flink-jobs.jar \
        --sidecar-argument=--arguments="--INPUT A --OUTPUT B"

### How to cancel a job

Execute the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar \
        cancel \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --create-savepoint \
        --job-id=your-job-id

Show more parameters with the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar cancel --help

### How to list the jobs

Execute the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar \
        list \
        --cluster-name=my-flink-cluster \
        --environment=test

Show more parameters with the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar list --help

## More about server application and sidecar controller

The server application and the sidecar controller are usually executed a containers.
However it might be necessary to run the server and the controller manually for testing.     

### How to run the server application

Run the server application within Kubernetes:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar server

Run the server application outside Kubernetes:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar server --port=4444 --kube-config=/your-kube-config.conf

Show more parameters with the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar server --help

### How to run the sidecar controller

Run the sidecar controller within Kubernetes:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar sidecar watch --cluster-name=test

Run the sidecar controller outside Kubernetes:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar sidecar watch --cluster-name=test --kube-config=/your-kube-config.conf

Show more parameters with the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar sidecar watch --help

### How to submit a job from the sidecar controller

Run the sidecar controller within Kubernetes:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar sidecar submit --cluster-name=test

Run the sidecar controller outside Kubernetes:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar sidecar submit --cluster-name=test --kube-config=/your-kube-config.conf --class-name=your-main-class --jar-path=/your-job-jar.jar

Show more parameters with the command:

    java -jar com.nextbreakpoint.flinksubmit-1.0.0-alpha.jar sidecar submit --help
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/your-job-jar.jar \
        --sidecar-argument=--arguments="--input A --output B"
