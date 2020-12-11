# Flink Kubernetes Toolbox

Flink Kubernetes Toolbox is a CLI tool for deploying and managing Apache Flink on Kubernetes.
The toolbox provides a native command flinkctl which can be executed on any Linux or MacOS machine and from a Docker container.
The command implements both client and server components which together represent a complete solution for operating Apache Flink on Kubernetes.
The command is based on the Kubernetes Operator Pattern, it implements a custom controller and it works with custom resources.         

Main features:

- Automatically manage separate supervisor for each cluster
- Automatically create or delete JobManagers and TaskManagers
- Automatically create or delete jobs in Flink server
- Automatically recover from temporary failure
- Automatically restart clusters or jobs when resource changed
- Automatically create a savepoint before stopping clusters or jobs
- Automatically recover from latest savepoint when restarting a job
- Support scaling based on standard Kubernetes scaling interface
- Support for deployment, cluster and job resources
- Support for batch and stream jobs
- Support for init containers and side containers for JobManagers and TaskManagers
- Support for mounted volumes (same as volumes in Pod specification)
- Support for environment variables, including variables from ConfigMap or Secret
- Support for resource requirements (for all components)
- Support for user defined annotations
- Support for user defined container ports
- Support for pull secrets and private registries
- Support for public Flink images or custom images
- Support for cluster without jobs (bare cluster)
- Support for cluster with one or more jobs
- Use separate Docker image for launching a job (single JAR file)
- Configurable service accounts
- Configurable periodic savepoints
- Configurable savepoints location
- CLI and REST interface to support operations
- Metrics compatible with Prometheus
- Resource status and printer columns
- Readiness probe for JobManager

## License

The tools are distributed under the terms of BSD 3-Clause License.

    Copyright (c) 2020, Andrea Medeghini
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

## Overview

At the core of the toolbox there is a Kubernetes operator.

In a nutshell, the operator detects changes to custom resources and applies modifications to some other resources or,
it controls instances of Apache Flink via the Flink Monitoring REST API in order to converge to the desired status.

Currently, the operator support 3 custom resources: FlinkDeployment, FlinkCluster and FlinkJob.

The FlinkDeployment resource can be used to define a FlinkCluster with multiple FlinkJobs in a single resource.
The FlinkDeployment is convenient to use but, it is not required. FlinkCluster and FlinkJob can be created separately.
The only requirement is that the name of the FlinkJob resource must start with the name of the corresponding FlinkCluster
resource followed by - and the job name, like clustername-jobname.

The operator can be installed in a separate namespace from the one where Flink is executed, provided the correct permissions.  

The operator detects changes in the primary resources, which are the custom resources, and eventually
it creates or deletes one or more secondary resources, such as Pods, Services and BatchJob.  

The operator persists its status in the custom resources, and the status can be inspected with kubectl or directly with flinkctl.

The operator can perform several tasks automatically, such as creating savepoints when a job is restarted
or recreating the cluster when the specification changed, which make easier to operate Flink on Kubernetes.  

The operator creates and manages a separate supervisor process for each cluster.
The supervisor is responsible to reconcile the status of the cluster and its jobs.
Jobs can be added or removed to an existing cluster either updating a deployment resource
or directly creating or deleting new job resources.

Here is the picture showing the dependencies between primary and secondary resources:

![Operator dependencies](/graphs/flink-operator.png "Operator dependencies")

Here is the picture showing the state machine for a FlinkDeployment resource:

![FlinkDeployment state machine](/graphs/flink-deployment.png "FlinkDeployment state machine")

Here is the picture showing the state machine for a FlinkCluster resource:

![FlinkCluster state machine](/graphs/flink-cluster.png "FlinkCluster state machine")

Here is the picture showing the state machine for a FlinkJob resource:

![FlinkJob state machine](/graphs/flink-job.png "FlinkJob state machine")





## Generate SSL certificates and keystores

Execute the script secrets.sh to generate self-signed certificates and keystores to use with the operator:

    ./secrets.sh flink-operator key-password keystore-password truststore-password

This command will generate new certificates and keystores in the directory secrets.

## Install toolbox

Create a namespace for the operator:

    kubectl create namespace flink-operator

The name of the namespace can be any name you like.

Create a namespace for executing the Flink jobs:

    kubectl create namespace flink-jobs

The name of the namespace can be any name you like.

Create a secret which contains the keystore and the truststore files:

    kubectl -n flink-operator create secret generic flink-operator-ssl \
        --from-file=keystore.jks=secrets/keystore-operator-api.jks \
        --from-file=truststore.jks=secrets/truststore-operator-api.jks \
        --from-literal=keystore-secret=keystore-password \
        --from-literal=truststore-secret=truststore-password

The name of the secret can be any name you like.

Install the CRDs (Custom Resource Definitions) with Helm command:

    helm install flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd

Install the default roles with Helm command:

    helm install flink-k8s-toolbox-roles helm/flink-k8s-toolbox-roles --namespace flink-jobs

Install the operator with SSL enabled:

    helm install flink-k8s-toolbox-operator helm/flink-k8s-toolbox-operator --namespace flink-operator --set namespace=flink-jobs --set secretName=flink-operator-ssl

Or if you prefer install the operator with SSL disabled:

    helm install flink-k8s-toolbox-operator helm/flink-k8s-toolbox-operator --namespace flink-operator --set namespace=flink-jobs

Scale the operator with command (only one replica is currently supported):

    kubectl -n flink-operator scale deployment flink-operator --replicas=1

Alternatively, you can pass the argument --set replicas=1 when installing the operator with Helm.

## Uninstall toolbox

Delete all FlinkDeployment resources:

    kubectl -n flink-jobs delete fd --all

and wait until there aren't any FlinkDeployment resources.

Delete all FlinkCluster resource:

    kubectl -n flink-jobs delete fc --all

and wait until there aren't any FlinkCluster resources.

Delete all FlinkJob resource:

    kubectl -n flink-jobs delete fj --all

and wait until there aren't any FlinkJob resources.

It might happen that the operator has an issue and some resources can't be removed until there are finalizers pending.
If you are in that situation you can always manually remove the finalizers to allow Kubernetes to remove all resources.
However, remember that without finalizers the operator might not be able to properly terminate all resources,
which ultimately have to be removed manually.

Stop the operator with command:

    kubectl -n flink-operator scale deployment flink-operator --replicas=0

Remove the operator with command:    

    helm uninstall flink-k8s-toolbox-operator --namespace flink-operator

Remove the default roles with command:    

    helm uninstall flink-k8s-toolbox-roles --namespace flink-jobs

Remove the CRDs with command:    

    helm uninstall flink-k8s-toolbox-crd

Remove secret with command:    

    kubectl -n flink-operator delete secret flink-operator-ssl

Remove operator namespace with command:    

    kubectl delete namespace flink-operator

Remove Flink jobs namespace with command:    

    kubectl delete namespace flink-jobs

## Upgrade toolbox

PLEASE NOTE THAT THE OPERATOR IS STILL IN BETA VERSION AND IT DOESN'T HAVE A STABLE API YET, THEREFORE EACH RELEASE MIGHT INTRODUCE BREAKING CHANGES.

Before upgrading to a new release you must cancel all jobs creating a savepoint into a durable storage location (for instance AWS S3).

Create a copy of your FlinkDeployment resources:

    kubectl -n flink-operator get fd -o yaml > deployments-backup.yaml

Create a copy of your FlinkCluster resources:

    kubectl -n flink-operator get fc -o yaml > clusters-backup.yaml

Create a copy of your FlinkJob resources:

    kubectl -n flink-operator get fj -o yaml > jobs-backup.yaml

Upgrade the default roles using Helm:

    helm upgrade flink-k8s-toolbox-roles --install helm/flink-k8s-toolbox-roles --namespace flink-jobs

Upgrade the CRDs using Helm (upgrade prevents from deleting the existing resources, but it might not work all the times):

    helm upgrade flink-k8s-toolbox-crd --install helm/flink-k8s-toolbox-crd

After installing the new CRDs, you can recreate all the custom resources.
However, the old resources might not be compatible with the new CRDs.
If that is the case, then you have to fix each resource's specification.

Finally, upgrade and restart the operator using Helm:

    helm upgrade flink-k8s-toolbox-operator --install helm/flink-k8s-toolbox-operator --namespace flink-operator --set namespace=flink-jobs --set secretName=flink-operator-ssl --set replicas=1

## Custom resources

FlinkDeployment, FlinkCluster and FlinkJob resources can be created, deleted, and inspected using kubectl command as any other Kubernetes's resource.

The Custom Resource Definitions are installed with a separate Helm chart:

    helm install flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd

The CRDs and their schemas are defined in the Helm templates:

    https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/helm/flink-k8s-toolbox-crd/templates/flinkdeployment.yaml
    https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/helm/flink-k8s-toolbox-crd/templates/flinkcluster.yaml
    https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/helm/flink-k8s-toolbox-crd/templates/flinkjob.yaml

It is recommended to upgrade the CRDs instead of deleting and recreating them:

    helm upgrade flink-k8s-toolbox-crd --install helm/flink-k8s-toolbox-crd  

Do not delete the CRDs unless you are happy to delete all custom resources depending on them.

## Docker image

The Docker image with flinkctl command can be downloaded from Docker Hub:

    docker pull nextbreakpoint/flinkctl:1.4.0-beta

### Create your first deployment

Make sure that CRDs and default roles have been installed in Kubernetes (see above).

Create a file Dockerfile:

    FROM nextbreakpoint/flinkctl:1.4.0-beta
    COPY flink-jobs.jar /flink-jobs.jar

where flink-jobs.jar contains the code of your Flink jobs.

Build the Docker image with command:

    docker build -t jobs:latest .

Pull the Flink's Docker image with command:

    docker pull flink:1.9.2

Create a file deployment.yaml:

    apiVersion: "nextbreakpoint.com/v1"
    kind: FlinkCluster
    metadata:
      name: test
    spec:

Create the resource with command:

    kubectl -n flink-jobs apply -f deployment.yaml

Please note that you can use any image of Flink as far as the image
implements the standard commands for running JobManager and TaskManager.

At this point the operator should create a bunch of resources,
deploy JobManager and TaskManagers, and run the Flink jobs.

You can observe what the operator is doing:

You can observe what the supervisor is doing:

You can watch the FlinkDeployment resource:

You can watch the FlinkCluster resource:

You can watch the FlinkJob resources:

You can watch the pods:



List custom objects of type FlinkCluster with command:

    kubectl -n flink-jobs get flinkclusters

The command should produce an output like:

    NAME   CLUSTER-STATUS   TASK-STATUS   TASK-MANAGERS   TASK-SLOTS   ACTIVE-TASK-MANAGERS   TOTAL-TASK-SLOTS   JOB-PARALLELISM   JOB-RESTART   SERVICE-MODE   SAVEPOINT-MODE   SAVEPOINT-PATH                                       SAVEPOINT-AGE   AGE
    test   Running          Idle          1               1            1                      1                  1                 Always        NodePort       Manual           file:/var/savepoints/savepoint-e0e430-7a6d1c33dee3   42s             3m55s

You can inspect the FlinkDeployment resource:

You can inspect the FlinkCluster resource:

You can inspect the FlinkJob resources:





## Control interface

The operator exposes a REST control interface on port 4444 by default.

The control interface provides information like status, details and metrics,
and it allows to submit commands which can be understood by the operator.

These endpoints support GET requests:

    http://localhost:4444/clusters
    http://localhost:4444/cluster/<name>/status
    http://localhost:4444/cluster/<name>/job/details
    http://localhost:4444/cluster/<name>/job/metrics
    http://localhost:4444/cluster/<name>/jobmanager/metrics
    http://localhost:4444/cluster/<name>/taskmanagers
    http://localhost:4444/cluster/<name>/taskmanagers/<taskmanager>/metrics

These endpoints support PUT requests:

    http://localhost:4444/clusters

These endpoints support POST requests:

    http://localhost:4444/clusters

These endpoints support DELETE requests:

    http://localhost:4444/clusters

Please note that you must use SSL certificates when invoking the API if operator
has SSL enabled (see instructions for generating SSL certificates above):

    curl --cacert secrets/ca_cert.pem --cert secrets/operator-cli_cert.pem --key secrets/operator-cli_key.pem https://localhost:4444/cluster/test/status



## How to use flinkctl

Print the CLI usage:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.0-beta --help

The output should look like:

    Usage: flink-k8s-toolbox [OPTIONS] COMMAND [ARGS]...

    Options:
      -h, --help  Show this message and exit

    Commands:
      operator      Access operator subcommands
      clusters      Access clusters subcommands
      cluster       Access cluster subcommands
      savepoint     Access savepoint subcommands
      bootstrap     Access bootstrap subcommands
      job           Access job subcommands
      jobmanager    Access JobManager subcommands
      taskmanager   Access TaskManager subcommands
      taskmanagers  Access TaskManagers subcommands

### How to create a cluster

Create a Docker file like:

    FROM nextbreakpoint/flinkctl:1.4.0-beta
    COPY flink-jobs.jar /flink-jobs.jar

where flink-jobs.jar contains the code of your Flink job.

Create a Docker image:

    docker build -t jobs:latest .

Tag and push the image into your registry if needed:

    docker tag jobs:latest some-registry/jobs:latest
    docker login some-registry
    docker push some-registry/jobs:latest

Create a JSON file:

    cat <<EOF >test.json
    {
      "taskManagers": 1,
      "runtime": {
        "pullPolicy": "Always",
        "image": "some-registry/flink:1.9.2"
      },
      "bootstrap": {
        "pullPolicy": "Always",
        "image": "some-registry/jobs:latest",
        "jarPath": "/flink-jobs.jar",
        "className": "com.nextbreakpoint.flink.jobs.stream.TestJob",
        "arguments": [
          "--DEVELOP_MODE",
          "disabled"
        ]
      },
      "jobManager": {
        "serviceMode": "NodePort",
        "environment": [
          {
            "name": "FLINK_JM_HEAP",
            "value": "256"
          },
          {
            "name": "FLINK_GRAPHITE_HOST",
            "value": "graphite.default.svc.cluster.local"
          }
        ],
        "environmentFrom": [
          {
            "secretRef": {
              "name": "flink-secrets"
            }
          }
        ],
        "volumes": [
          {
            "name": "config-vol",
            "configMap": {
              "name": "flink-config",
              "defaultMode": "511"
            }
          }
        ]
      },
      "taskManager": {
        "environment": [
          {
            "name": "FLINK_TM_HEAP",
            "value": "1024"
          },
          {
            "name": "FLINK_GRAPHITE_HOST",
            "value": "graphite.default.svc.cluster.local"
          }
        ],
        "volumes": [
          {
            "name": "config-vol",
            "configMap": {
              "name": "flink-config",
              "defaultMode": "511"
            }
          },
          {
            "name": "data-vol",
            "hostPath": {
              "path": "/var/data"
            }
          }
        ]
      },
      "operator": {
        "savepointMode": "Automatic",
        "savepointInterval": 300,
        "savepointTargetPath": "file:///var/tmp/test",
        "restartPolicy": "Never"
      }
    }
    EOF

Execute the command:

    docker run --rm -it flinkctl:1.4.0-beta cluster create --cluster-name=test --cluster-spec=test.json --host=$OPERATOR_HOST --port=4444

Pass keystore and truststore if SSL is enabled:

    docker run --rm -it flinkctl:1.4.0-beta cluster create --cluster-name=test --cluster-spec=test.json --host=$OPERATOR_HOST --port=4444
    --keystore-path=secrets/keystore-operator-cli.jks --truststore-path=secrets/truststore-operator-cli.jks --keystore-secret=keystore-password --truststore-secret=truststore-password

If you expose the operator on a port of Docker's host:

        Set OPERATOR_HOST to localhost on Linux

        Set OPERATOR_HOST to host.docker.internal on MacOS

Show more options with the command:

    docker run --rm -it flinkctl:1.4.0-beta cluster create --help

Get the list of clusters

    docker run --rm -it flinkctl:1.4.0-beta clusters list --host=$OPERATOR_HOST --port=4444

Get the status of a cluster

    docker run --rm -it flinkctl:1.4.0-beta cluster status --cluster-name=test --host=$OPERATOR_HOST --port=4444

Use jq to format the output:

    docker run --rm -it flinkctl:1.4.0-beta cluster status --cluster-name=test --host=$OPERATOR_HOST --port=4444 | jq -r

Delete a cluster

    docker run --rm -it flinkctl:1.4.0-beta cluster delete --cluster-name=test --host=$OPERATOR_HOST --port=4444

Stop a running cluster

    docker run --rm -it flinkctl:1.4.0-beta cluster stop --cluster-name=test --host=$OPERATOR_HOST --port=4444

Restart a stopped or failed cluster

    docker run --rm -it flinkctl:1.4.0-beta cluster start --cluster-name=test --host=$OPERATOR_HOST --port=4444

Start a cluster and run the job without savepoint

    docker run --rm -it flinkctl:1.4.0-beta cluster start --cluster-name=test --without-savepoint --host=$OPERATOR_HOST --port=4444

Stop a cluster without creating a savepoint

    docker run --rm -it flinkctl:1.4.0-beta cluster stop --cluster-name=test --without-savepoint --host=$OPERATOR_HOST --port=4444

Create a new savepoint

    docker run --rm -it flinkctl:1.4.0-beta savepoint trigger --cluster-name=test --host=$OPERATOR_HOST --port=4444

Get the status of a cluster

    docker run --rm -it flinkctl:1.4.0-beta cluster status --cluster-name=test --host=$OPERATOR_HOST --port=4444

Rescale a cluster (with savepoint)

    docker run --rm -it flinkctl:1.4.0-beta cluster scale --cluster-name=test --task-managers=4 --host=$OPERATOR_HOST --port=4444

Get the details of the running job

    docker run --rm -it flinkctl:1.4.0-beta job details --cluster-name=test --host=$OPERATOR_HOST --port=4444

Get the metrics of the running job

    docker run --rm -it flinkctl:1.4.0-beta job metrics --cluster-name=test --host=$OPERATOR_HOST --port=4444

Get a list of Task Managers

    docker run --rm -it flinkctl:1.4.0-beta taskmanagers list --cluster-name=test --host=$OPERATOR_HOST --port=4444

Get the metrics of the Job Manager

    docker run --rm -it flinkctl:1.4.0-beta jobmanager metrics --cluster-name=test --host=$OPERATOR_HOST --port=4444

Get the metrics of a Task Manager

    docker run --rm -it flinkctl:1.4.0-beta taskmanager metrics --cluster-name=test --host=$OPERATOR_HOST --port=4444

You will be asked to provide a Task Manager id which you can get from the list of Task Managers.   

Get the details of a Task Manager

    docker run --rm -it flinkctl:1.4.0-beta taskmanager details --cluster-name=test --host=$OPERATOR_HOST --port=4444

You will be asked to provide a Task Manager id which you can get from the list of Task Managers.   




## How to upload a JAR file and start a job

Flink jobs must be packaged in a regular JAR file and uploaded to the JobManager.

Upload a JAR file using the command:

    java -jar flink-k8s-toolbox-1.4.0-beta.jar bootstrap run --cluster-name=test --class-name=your-main-class --jar-path=/your-job-jar.jar

When running outside Kubernetes use the command:

    java -jar flink-k8s-toolbox-1.4.0-beta.jar bootstrap run --kube-config=/your-kube-config --cluster-name=test --class-name=your-main-class --jar-path=/your-job-jar.jar



## Test and debug

The Flink Operator can be executed as Docker image or JAR file, pointing to a local or remote Kubernetes cluster.    

Run the operator with a given namespace and Kubernetes config using the JAR file:

    java -jar flinkctl:1.4.0-beta.jar operator run --namespace=test --kube-config=~/.kube/config

Run the operator with a given namespace and Kubernetes config using the Docker image:

    docker run --rm -it -v ~/.kube/config:/kube/config flinkctl:1.4.0-beta operator run --namespace=test --kube-config=/kube/config





## Build with Gradle

Make sure you have Java 11 installed and java is in your command path.

Compile and package the code with command:

    ./gradlew build copyRuntimeDeps

## Build with Docker

Build a Docker image with command:

    docker build -t flinkctl:1.4.0-beta .

Test the image printing the CLI usage:

    docker run --rm -it flinkctl:1.4.0-beta --help

Tag and push the image to your Docker registry if needed:

    docker tag flinkctl:1.4.0-beta some-registry/flinkctl:1.4.0-beta
    docker login some-registry
    docker push some-registry/flinkctl:1.4.0-beta

## Run automated tests

Run unit tests with command:

    ./gradlew clean test

Run integration tests against Docker for Desktop or Minikube with command:

    export BUILD_IMAGES=true
    ./gradlew clean integrationTest

You can skip the Docker images build step if images already exist:

    export BUILD_IMAGES=false
    ./gradlew clean integrationTest





## Monitoring

The operator process exposes metrics to Prometheus on port 8080 by default:

    http://localhost:8080/metrics

The supervisor process exposes metrics to Prometheus on port 8080 by default:

    http://localhost:8080/metrics

### Configure service accounts

The service account of JobManagers and TaskManagers can be configured in the FlinkDeployment and FlinkCluster resources:

    jobManager:
      serviceAcount: some-account

    taskManager:
      serviceAcount: some-account

If not specified, the default service account will be used for JobManagers and TaskManagers.

The service account of the Supervisor can be configured in the FlinkDeployment and FlinkCluster resources:

    supervisor:
      serviceAcount: some-account

If not specified, the service account flink-supervisor will be used for the Supervisor.

The service account of the Bootstrap can be configured in the FlinkDeployment and FlinkJob resources:

    bootstrap:
      serviceAccount: flink-bootstrap

If not specified, the service account flink-bootstrap will be used for the Bootstrap.

## Automatic savepoints

The operator automatically creates savepoints before stopping the cluster.
This might happen when a change is applied to the cluster specification or
the cluster is rescaled or manually stopped. This feature is very handy to
avoid losing the status of the job.
When the operator restarts the cluster, it uses the latest savepoint to
recover the status of the job. However, for this feature to work properly,
the savepoints must be created in a durable storage location such as HDFS or S3.
Only a durable location can be used to recover the job after recreating
the Job Manager and the Task Managers.

## Configure task timeout

The Flink Operator uses timeouts to recover for anomalies.

The duration of the timeout has a default value of 300 seconds and can be changed setting the environment variable TASK_TIMEOUT (number of seconds).   

## Configure polling interval

The Flink Operator polls periodically the status of the resources.

The polling interval has a default value of 5 seconds and can be changed setting the environment variable POLLING_INTERVAL (number of seconds).   
