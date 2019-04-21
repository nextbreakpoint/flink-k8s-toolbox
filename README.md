# Flink Kubernetes Toolbox

Flink K8S Toolbox contains a set of tools for managing Flink clusters on Kubernetes.
It includes a CLI tool, a controller, a sidecar, and a Kubernetes operator:
- The command-line interface tool interprets commands for managing clusters and jobs.
- The controller accepts commands from the CLI tool and executes operations against Kubernetes and Flink clusters.
- The sidecar is responsible of launching and monitoring a job in a Flink cluster.
- The Kubernetes operator creates or deletes Flink clusters based on custom resources.

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

    docker fetch nextbreakpoint/flink-k8s-toolbox:1.0.0-alpha

Tag and push the image into your registry if required:

    docker tag nextbreakpoint/flink-k8s-toolbox:1.0.0-alpha some-registry/flink-k8s-toolbox:1.0.0-alpha

    docker login some-registry

    docker push some-registry/flink-k8s-toolbox:1.0.0-alpha

## Install Flink Controller

Create service account and RBAC role:

    kubectl create -f flink-controller-rbac.yaml

Verify that the service account has been created:

    kubectl get serviceaccounts flink-controller -o yaml     

Run the controller using the image on Docker Hub:

    kubectl run flink-controller --restart=Never --image=nextbreakpoint/flink-k8s-toolbox:1.0.0-alpha --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-controller" } }, "spec": { "serviceAccountName": "flink-controller", "imagePullPolicy": "Always" } }'

Or run the controller using your own registry:

    kubectl run flink-controller --restart=Never --image=some-registry/flink-k8s-toolbox:1.0.0-alpha --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-controller" } }, "spec": { "serviceAccountName": "flink-controller", "imagePullPolicy": "Always", "imagePullSecrets": [{"name": "your-pull-secrets"}] } }'

Verify that the pod has been created:

    kubectl get pod flink-controller -o yaml     

The pod must have a label app with value *flink-controller* and it must run with *flink-controller* service account.

Verify that there are no errors in the logs:

    kubectl logs flink-controller

Check the system events if the pod doesn't start:

    kubectl get events

## Install Flink Operator

Create service account and RBAC role:

    kubectl create -f flink-operator-rbac.yaml

Verify that the service account has been created:

    kubectl get serviceaccounts flink-operator -o yaml     

Run the operator using the image on Docker Hub:

    kubectl run flink-operator --restart=Never --image=nextbreakpoint/flink-k8s-toolbox:1.0.0-alpha --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always" } }' -- operator run --namespace=test

Or run the operator using your own registry:

    kubectl run flink-operator --restart=Never --image=some-registry/flink-k8s-toolbox:1.0.0-alpha --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always", "imagePullSecrets": [{"name": "your-pull-secrets"}] } }' -- operator run --namespace=test

Run one operator for each namespace.

Verify that the pod has been created:

    kubectl get pod flink-operator -o yaml     

The pod must have a label app with value *flink-operator* and it must run with *flink-operator* service account.

Verify that there are no errors in the logs:

    kubectl logs flink-operator

Check the system events if the pod doesn't start:

    kubectl get events

## Create Custom Resource Definition

Flink Operator requires a CRD (Custom Resource Definition):

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

Create the CRD with command:

    kubectl create -f flink-operator-crd.yaml

## Create Custom Object representing Flink cluster

Make sure the CDR has been installed (see above).

Create a Docker file like:

    FROM nextbreakpoint/flink-k8s-toolbox:1.0.0-alpha
    COPY flink-jobs.jar /flink-jobs.jar

where flink-jobs.jar contains the code of your Flink jobs.

Create a Docker image:

    docker build -t flink-submit-with-jobs:1.0.0 .

Tag and push the image into your registry if required:

    docker tag flink-submit-with-jobs:1.0.0 some-registry/flink-submit-with-jobs:1.0.0

    docker login some-registry

    docker push some-registry/flink-submit-with-jobs:1.0.0

Create a resource file:

    cat <<EOF >flink-cluster-test.yaml
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
    EOF

Create the custom object with command:

    kubectl create -f flink-cluster-test.yaml

## Delete Custom Object representing Flink cluster

Delete the custom object with command:

    kubectl delete -f flink-cluster-test.yaml

## List Custom Objects representing Flink clusters

List all custom objects with command:

    kubectl get flinkclusters

## Build from source code

Build the tools using Maven:

    mvn clean package

Maven will create a fat JAR and a Docker image.

Create a tag and push the image to your Docker registry:

    docker tag flink-k8s-toolbox:1.0.0-alpha some-registry/flink-k8s-toolbox:1.0.0-alpha

    docker login some-registry

    docker push some-registry/flink-k8s-toolbox:1.0.0-alpha

## How to use the CLI tool

CLI tool can be executed as Docker image or as JAR file.   

Show all available commands using the JAR file:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar --help

Or show all available commands using the Docker image:

    docker run --rm -it nextbreakpoint/flink-k8s-toolbox:1.0.0-alpha --help

The output should look like:

    Usage: flink-k8s-toolbox [OPTIONS] COMMAND [ARGS]...

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

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar \
        cluster \
        create \
        --cluster-name=test \
        --image-pull-secrets=regcred \
        --flink-image=nextbreakpoint/flink:1.7.2-1 \
        --sidecar-image=nextbreakpoint/flink-k8s-toolbox:1.0.0-alpha \
        --sidecar-arguments="watch --cluster-name=test"

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar cluster create --help

### How to create a cluster and submit a job

Create a Docker file:

    cat <<EOF >Dockerfile
    FROM nextbreakpoint/flink-k8s-toolbox:1.0.0-alpha
    COPY flink-jobs.jar /flink-jobs.jar
    EOF

where flink-jobs.jar contains the code of your Flink jobs.

Create a Docker image:

    docker build -t flink-k8s-toolbox-with-jobs:1.0.0 .

Create a tag and push the image to your Docker registry:

    docker tag flink-k8s-toolbox-with-jobs:1.0.0 some-registry/flink-k8s-toolbox-with-jobs:1.0.0

    docker login some-registry

    docker push some-registry/flink-k8s-toolbox-with-jobs:1.0.0

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar \
        cluster \
        create \
        --cluster-name=test \
        --image-pull-secrets=regcred \
        --flink-image=nextbreakpoint/flink:1.7.2-1 \
        --sidecar-image=some-registry/flink-k8s-toolbox-with-jobs:1.0.0 \
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

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar \
        cluster \
        delete \
        --cluster-name=my-flink-cluster \
        --environment=test

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar cluster delete --help

### How to run a job

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar \
        job \
        run \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --image-pull-secrets=regcred \
        --sidecar-image=some-registry/flink-k8s-toolbox-with-jobs:1.0.0 \
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/flink-jobs.jar

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar job run --help

### How to pass multiple arguments to a job

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar \
        job \
        run \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --sidecar-image=some-registry/flink-k8s-toolbox-with-jobs:1.0.0 \
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

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar \
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

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar \
        job \
        cancel \
        --cluster-name=my-flink-cluster \
        --environment=test \
        --create-savepoint \
        --job-id=your-job-id

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar job cancel --help

### How to list the jobs

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar \
        jobs \
        list \
        --cluster-name=my-flink-cluster \
        --environment=test

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar jobs list --help

## More about controller, operator and sidecar

The controller and the sidecar are usually executed a containers.
However it might be necessary to run the controller and the sidecar manually for testing.     

### How to run the controller

Run the controller within Kubernetes:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar controller run

Run the controller outside Kubernetes:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar controller run --port=4444 --kube-config=/your-kube-config.conf

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar controller run --help

### How to run the sidecar

Run the sidecar within Kubernetes:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar sidecar watch --cluster-name=test

Run the sidecar outside Kubernetes:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar sidecar watch --kube-config=/your-kube-config.conf --cluster-name=test

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar sidecar watch --help

### How to submit a job from the sidecar

Run the sidecar within Kubernetes:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar sidecar submit --cluster-name=test --class-name=your-main-class --jar-path=/your-job-jar.jar

Run the sidecar outside Kubernetes:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar sidecar submit --kube-config=/your-kube-config.conf --cluster-name=test --class-name=your-main-class --jar-path=/your-job-jar.jar

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.0.0-alpha.jar sidecar submit --help
        --sidecar-argument=submit \
        --sidecar-argument=--cluster-name=test \
        --sidecar-argument=--class-name=your-main-class \
        --sidecar-argument=--jar-path=/your-job-jar.jar \
        --sidecar-argument=--arguments="--input A --output B"

### How to run the operator

The operator can be executed as Docker image or as JAR file.   

Run the operator with a given namespace and Kubernetes config using the JAR file:

    java -jar com.nextbreakpoint.flink-k8s-toolbox:1.0.0-alpha.jar operator run --namespace=test --kube-config=/path/admin.conf

Run the operator with a given namespace and Kubernetes config using the Docker image:

    docker run --rm -it -v /path/admin.conf:/admin.conf flink-k8s-toolbox:1.0.0-alpha operator run --namespace=test --kube-config=/admin.conf
