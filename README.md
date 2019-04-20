# flink-k8-ops

Flink K8 Ops contains a set of tools for managing Flink clusters on Kubernetes.

The set of tools includes a CLI, a controller application, a Kubernetes operator, and a sidecar application:

- The command-line interface interprets commands for managing clusters and jobs.
- The controller application accepts requests from the CLI and executes commands against Kubernetes and Flink.
- The operator creates or deletes clusters according to custom resources.
- The sidecar application is responsible of deploying and monitoring a job.        

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

    docker fetch nextbreakpoint/flink-k8-ops:1.0.1-alpha

Tag and push the image into your registry if required:

    docker tag nextbreakpoint/flink-k8-ops:1.0.1-alpha some-registry/flink-k8-ops:1.0.1-alpha

    docker login some-registry

    docker push some-registry/flink-k8-ops:1.0.1-alpha

## Install Flink Controller

Create service account and RBAC role:

    kubectl create -f flink-k8-ops-rbac.yaml

Verify that service account has been created:

    kubectl get serviceaccounts flink-k8-ops -o yaml     

Run the controller using Docker Hub:

    kubectl run flink-controller --restart=Never --image=nextbreakpoint/flink-k8-ops:1.0.1-alpha --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-controller" } }, "spec": { "serviceAccountName": "flink-controller", "imagePullPolicy": "Always" } }'

Or run the controller using your registry:

    kubectl run flink-controller --restart=Never --image=some-registry/flink-k8-ops:1.0.1-alpha --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-controller" } }, "spec": { "serviceAccountName": "flink-controller", "imagePullPolicy": "Always", "imagePullSecrets": [{"name": "your-pull-secrets"}] } }'

Verify that pod has been created:

    kubectl get pod flink-controller -o yaml     

The pod must have a label app with value *flink-controller* and it must run with *flink-controller* service account.

Verify that there are no errors in the logs:

    kubectl logs flink-k8-ops

Check the system events if the pod doesn't start:

    kubectl get events

## Install Flink Operator

Create service account and RBAC role:

    kubectl create -f flink-operator-rbac.yaml

Verify that service account has been created:

    kubectl get serviceaccounts flink-operator -o yaml     

Run the operator using Docker Hub:

    kubectl run flink-operator --restart=Never --image=nextbreakpoint/flink-k8-ops:1.0.1-alpha --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always" } }' -- operator run --namespace=test

Or run the operator using your registry:

    kubectl run flink-operator --restart=Never --image=some-registry/flink-k8-ops:1.0.1-alpha --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always", "imagePullSecrets": [{"name": "your-pull-secrets"}] } }' -- operator run --namespace=test

Run one operator for each namespace.

Verify that pod has been created:

    kubectl get pod flink-operator -o yaml     

The pod must have a label app with value *flink-operator* and it must run with *flink-operator* service account.

Verify that there are no errors in the logs:

    kubectl logs flink-operator

Check the system events if the pod doesn't start:

    kubectl get events

## Create Custom Resource Definition

Flink Operator requires a Custom Resource Definition or CRD:

    apiVersion: apiextensions.k8s.io/v1beta1
    kind: CustomResourceDefinition
    metadata:
      name: flinkclusters.beta.nextbreakpoint.com
    spec:
      group: beta.nextbreakpoint.com
      versions:
        - name: v1
          served: true
          storage: true
      scope: Namespaced
      names:
        plural: flinkclusters
        singular: flinkcluster
        kind: FlinkCluster
        shortNames:
        - fc

Create the CDR with command:

    kubectl create -f flink-operator-crd.yaml

## Create Custom Object

Make sure the CDR has been installed.

Create a Docker file like:

    FROM nextbreakpoint/flink-submit:1.0.0-alpha
    COPY flink-jobs.jar /flink-jobs.jar

Where flink-jobs.jar contains the code of your Flink jobs.

Create a Docker image:

    docker build -t flink-submit-with-jobs:1.0.0 .

Tag and push the image into your registry if required:

    docker tag flink-submit-with-jobs:1.0.0 some-registry/flink-submit-with-jobs:1.0.0

    docker login some-registry

    docker push some-registry/flink-submit-with-jobs:1.0.0

Create a resource file flink-cluster-test.yaml like:

    apiVersion: "beta.nextbreakpoint.com/v1"
    kind: FlinkCluster
    metadata:
      name: test
    spec:
      clusterName: test
      environment: test
      pullSecrets: regcred
      flinkImage: nextbreakpoint/flink:1.7.2-1
      sidecarImage: some-registry/flink-submit-with-jobs:1.0.0
      sidecarArguments:
       - submit
       - --cluster-name
       - test
       - --class-name
       - your-main-class
       - --jar-path
       - /flink-jobs.jar
       - --argument
       - --INPUT
       - --argument
       - A
       - --argument
       - --OUTPUT
       - --argument
       - B

Create the custom object with command:

    kubectl create -f flink-cluster-test.yaml

## Delete Custom Object

Delete the custom object with command:

    kubectl delete -f flink-cluster-test.yaml

## List Custom Objects

List the custom objects with command:

    kubectl get flinkclusters

## Build from source code

Build the tools using Maven:

    mvn clean package

Maven will create a fat jar and a Docker image.

Create a tag and push the image to your Docker registry:

    docker tag flink-k8-ops:1.0.1-alpha some-registry/flink-k8-ops:1.0.1-alpha

    docker login some-registry

    docker push some-registry/flink-k8-ops:1.0.1-alpha

## How to use the CLI

Execute the CLI using the Docker image or download the jar file and run the CLI with Java command.   

Show all commands using the jar file:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar --help

Or show all commands using the Docker image:

    docker run --rm -it nextbreakpoint/flink-k8-ops:1.0.1-alpha --help

The output should look like:

    Usage: Flink K8 Ops [OPTIONS] COMMAND [ARGS]...
    
    Options:
      -h, --help  Show this message and exit
    
    Commands:
      controller    Access controller subcommands
      operator      Access operator subcommands
      cluster       Access cluster subcommands
      sidecar       Access sidecar subcommands
      job           Access job subcommands
      jobs          Access jobs subcommands
      jobmanager    Access JobManager subcommands
      taskmanager   Access TaskManager subcommands
      taskmanagers  Access TaskManagers subcommands
      
### How to create a cluster

Execute the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar \
        cluster \
        create \
        --cluster-name=test \
        --flink-image=nextbreakpoint/flink:1.7.2-1 \
        --sidecar-image=nextbreakpoint/flink-k8-ops:1.0.1-alpha \
        --image-pull-secrets=regcred \
        --sidecar-arguments="watch --cluster-name=test"

Show more parameters with the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar cluster create --help

### How to create a cluster and submit a job

Create a Docker file:

    cat <<EOF >Dockerfile
    FROM nextbreakpoint/flink-k8-ops:1.0.1-alpha
    COPY flink-jobs.jar /flink-jobs.jar
    EOF

Where flink-jobs.jar contains the code of your Flink jobs.

Create a Docker image:

    docker build -t flink-k8-ops-with-jobs:1.0.0 .

Create a tag and push the image to your Docker registry:

    docker tag flink-k8-ops-with-jobs:1.0.0 some-registry/flink-k8-ops-with-jobs:1.0.0

    docker login some-registry

    docker push some-registry/flink-k8-ops-with-jobs:1.0.0

Execute the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar \
        cluster \
        create \
        --cluster-name=test \
        --flink-image=nextbreakpoint/flink:1.7.2-1 \
        --sidecar-image=some-registry/flink-k8-ops-with-jobs:1.0.0 \
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

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar \
        cluster \
        delete \
        --cluster-name=my-flink-cluster \
        --environment=test

Show more parameters with the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar cluster delete --help

### How to run a job

Execute the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar \
        job \
        run \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --sidecar-image=some-registry/flink-k8-ops-with-jobs:1.0.0 \
        --image-pull-secrets=regcred \
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/flink-jobs.jar

Show more parameters with the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar job run --help

### How to pass multiple arguments to a job

Execute the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar \
        job \
        run \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --sidecar-image=some-registry/flink-k8-ops-with-jobs:1.0.0 \
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

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar \
        job \
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

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar \
        job \
        cancel \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --create-savepoint \
        --job-id=your-job-id

Show more parameters with the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar job cancel --help

### How to list the jobs

Execute the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar \
        jobs \
        list \
        --cluster-name=my-flink-cluster \
        --environment=test

Show more parameters with the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar jobs list --help

## More about Flink controller, operator and sidecar

The controller application and the sidecar application are usually executed a containers.
However it might be necessary to run the controller and the sidecar manually for testing.     

### How to run the controller

Run the controller application within Kubernetes:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar controller run

Run the controller application outside Kubernetes:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar controller run --port=4444 --kube-config=/your-kube-config.conf

Show more parameters with the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar controller run --help

### How to run the sidecar

Run the sidecar application within Kubernetes:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar sidecar watch --cluster-name=test

Run the sidecar application outside Kubernetes:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar sidecar watch --cluster-name=test --kube-config=/your-kube-config.conf

Show more parameters with the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar sidecar watch --help

### How to submit a job from the sidecar

Run the sidecar application within Kubernetes:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar sidecar submit --cluster-name=test

Run the sidecar application outside Kubernetes:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar sidecar submit --cluster-name=test --kube-config=/your-kube-config.conf --class-name=your-main-class --jar-path=/your-job-jar.jar

Show more parameters with the command:

    java -jar com.nextbreakpoint.flink-k8-ops-1.0.1-alpha.jar sidecar submit --help
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/your-job-jar.jar \
        --sidecar-argument=--arguments="--input A --output B"

## How to run the operator

Execute the operator using the Docker image or download the jar file and run the operator with Java command.   

Show all commands using the jar file:

    java -jar com.nextbreakpoint.flink-k8-ops:1.0.1-alpha.jar operator --help

Or show all commands using the Docker image:

    docker run --rm -it nextbreakpoint/flink-k8-ops:1.0.1-alpha operator --help

The output should look like:

    Usage: operator [OPTIONS]

      Access operator subcommands

    Options:
      --namespace TEXT    The namespace where to create the resources
      --kube-config TEXT  The path of kuke config
      -h, --help          Show this message and exit

Run the operator with a given namespace:

    java -jar com.nextbreakpoint.flink-k8-ops:1.0.1-alpha.jar operator run --namespace=test
