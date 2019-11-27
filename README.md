# Flink Kubernetes Toolbox

Flink Kubernetes Toolbox contains tools for managing Flink clusters and jobs on Kubernetes:

- Flink Operator

    The Flink Operator is an implementation of the Kubernetes Operator pattern for managing Flink clusters and jobs.
    The operator uses a Custom Resource Definition to represent a cluster with a single job.
    It detects changes of the custom resource and modifies the derived resources which constitute the cluster.
    It takes care of creating savepoints periodically, monitoring the status of the job to detect a failure,
    restarting the job from the latest savepoint if needed, and rescaling the cluster when required.

- Flink Operator CLI

    The Flink Operator CLI provides an interface for controlling Flink clusters and jobs from a terminal.
    It supports commands for creating or deleting clusters, starting or stopping clusters and jobs,
    rescaling clusters, getting metrics and other information about clusters and jobs.

Main features:

- Automatic creation of JobManager and TaskManagers using StatefulSets
- Automatic creation of service for accessing JobManager
- Support for bare cluster or single job cluster
- Support for init containers and side containers for JobManager and TaskManagers
- Support for mounted volumes and persistent volumes claims
- Support for environment variables, including variables from config map
- Support for resource requirements
- Support for user defined annotations
- Support for user defined container ports
- Support for pull secrets and private registries
- Support for public Flink images or custom images
- Support for single job cluster or cluster without job
- Support for bootstrap from single JAR file
- Support for configurable task slots and heap memory
- Support for configurable savepoints location
- Configurable service accounts
- Configurable periodic savepoints
- Automatic detection of failure and configurable job restart
- Automatic scaling via standard autoscaling interface
- Automatic restart of the cluster or job when specification changed
- Automatic creation of savepoint before stopping cluster or job  
- Automatic recovery from latest savepoint when restarting job  
- Resource status and printer columns
- Readiness and Liveness probes for JobManager
- CLI interface for operations and monitoring   
- Internal metrics compatible with Prometheus  

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

## Flink Operator and Cluster status

The operator detects new resource of kind FlinkCluster (primary resource) in a namespace, and automatically creates other managed
resources (secondary resources), like StatefulSet, Service, and BatchJob, based on the specification provided in the custom resource.  
The operator persists some status on the resource, and performs several tasks automatically, such as creating savepoints
or recreating the cluster when the specification changed.

The possible states of a FlinkCluster resource are represented in this graph:

![Flink cluster status graph](/graphs/flink-cluster-status.dot.png "Flink cluster status graph")

- **UNKNOWN**

  This is the initial status of the Flink cluster when it is created.

- **STARTING**

  This status means that the cluster is starting up. The secondary resources will be created, the JAR file will be uploaded to Flink, and the job will be started (optional).

- **STOPPING**

  This status means that the cluster is going to stop. The Flink job will be canceled (creating a new savepoint) or stopped (without creating a new savepoint), and the secondary resources will be deleted (optional).

- **SUSPENDED**

  This status means that the cluster has been suspended. The Flink job has been stopped, and the secondary resources have been stopped.

- **TERMINATED**

  This status means that the cluster has been terminated. The Flink job has been stopped, and the secondary resources have been deleted.

- **RUNNING**

  This status means that the cluster is running. The secondary resources have been created and the job is running. When the cluster is in this state and the job stops running for any reason, the status is automatically changed to FAILED.

- **UPDATING**

  This status means that the cluster is updating the secondary resources. A change has been detected in the primary resource specification and the operator is modifying the secondary resources to reflect that change. The operator might need to destroy and recreate some resources.

- **FAILED**

  This status means that the cluster is not running properly. The secondary resources might not work or the job might have failed (or it has been canceled manually). When the cluster is in this state but a running job is detected, the status is automatically changed to RUNNING.

- **CHECKPOINTING**

  This status means that the cluster is creating a savepoint. The status is automatically changed to RUNNING when the savepoint is completed, otherwise the status is changed to FAILED (when the savepoint is not completed in the expected time).

Each arrow in this graph above represents a specific sequence of tasks which are executed in order to transition from one status to another. Each task of the sequence is processed according to this state machine:

![Task executor state machine](/graphs/task-executor-status.dot.png "Task executor state machine")

- **EXECUTING**

  This is the initial status of a task. The task is executing some operation against Kubernetes resources or Flink server. The task might repeat the operation several times before succeeding or timing out and generating a failure.

- **AWAITING**

  The task has executed some operations and it is now awaiting for a result. The task might check for results several times before succeeding or timing out and generating a failure.

- **IDLE**

  The task has received the expected result and it is now completed. The task is idle but it keeps checking for new tasks to execute and it can eventually schedule a new task.

- **FAILED**

  The task didn't received the expected result or some error occurred. The cluster status is changed to FAILED and the CLUSTER_HALTED task is scheduled for execution.

All the possible tasks which the operator can execute to transition from one status to another are:

- **INITIALISE CLUSTER**

  Initialise primary resource status and change cluster status to starting.    

- **CLUSTER RUNNING**

  Detect changes in the primary resource and restart cluster if needed. Change cluster status to FAILED if the job stops running. Periodically triggers a new savepoint.     

- **CLUSTER HALTED**

  Detect changes in the primary resource and restart cluster if needed. Change cluster status to RUNNING if there is a running job.    

- **STARTING CLUSTER**

  Set cluster status to STARTING.

- **STOPPING CLUSTER**

  Set cluster status to STOPPING.

- **UPDATING CLUSTER**

  Set cluster status to UPDATING.

- **RESCALE CLUSTER**

  Rescale cluster updating number of Task Managers.

- **CREATE RESOURCES**

  Create secondary resources and wait until resources reach expected status.

- **DELETE RESOURCES**

  Delete secondary resources and wait until resources reach expected status.

- **CREATE BOOTSTRAP JOB**

  Schedule batch job which upload JAR file to Flink.  

- **DELETE BOOTSTRAP JOB**

  Remove batch job previously used to upload JAR file.

- **TERMINATE PODS**

  Terminate pods scaling down resources (set replicas to 0).

- **RESTART PODS**

  Restart pods scaling up resources (set replicas to expected number of Task Managers).

- **CANCEL JOB**

  Cancel job creating a new savepoint and wait until savepoint is completed.  

- **START JOB**

  Start job using bootstrap configuration from primary resource. Restart job from savepoint when savepoint path is provided.   

- **STOP JOB**

  Cancel job without creating a savepoint.  

- **CREATING SAVEPOINT**

  Set cluster status to CHECKPOINTING.

- **TRIGGER SAVEPOINT**

  Trigger new savepoint and wait until savepoint is completed.

- **ERASE SAVEPOINT**

  Delete savepoint from status of primary resource.

## Flink Operator REST API

The operator exposes a REST API on port 4444 by default. The API provides information about the status of the resources, metrics of clusters and jobs, and more:

    http://localhost:4444/cluster/<name>/status

    http://localhost:4444/cluster/<name>/job/details

    http://localhost:4444/cluster/<name>/job/metrics

    http://localhost:4444/cluster/<name>/jobmanager/metrics

    http://localhost:4444/cluster/<name>/taskmanagers

    http://localhost:4444/cluster/<name>/taskmanagers/<taskmanager>/metrics

Please note that you must use SSL certificates when HTTPS is enabled (see instructions for generating SSL certificates):

    curl --cacert secrets/ca_cert.pem --cert secrets/operator-cli_cert.pem --key secrets/operator-cli_key.pem https://localhost:4444/cluster/test/status

## Generate SSL certificates and keystores

Execute the script secrets.sh to generate self-signed certificates and keystores to use with the Flink Operator:

    ./secrets.sh flink-operator key-password keystore-password truststore-password

This command will generate new certificates and keystores in the directory secrets.

## Monitor Flink Operator

The operator exposes metrics to Prometheus on port 8080 by default:

    http://localhost:8080/metrics

## Install Flink Operator

Create a namespace. Let's assume the namespace is flink, but you can use any name:

    kubectl create namespace flink

Create a secret which contain the keystore and the truststore files:

    kubectl -n flink create secret generic flink-operator-ssl \
        --from-file=keystore.jks=secrets/keystore-operator-api.jks \
        --from-file=truststore.jks=secrets/truststore-operator-api.jks \
        --from-literal=keystore-secret=keystore-password \
        --from-literal=truststore-secret=truststore-password

Install the operator's CRD resource with Helm command:

    helm install --name flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd

Install the operator's resources with SSL enabled:

    helm install --name flink-k8s-toolbox-operator --namespace flink helm/flink-k8s-toolbox-operator --set secretName=flink-operator-ssl  

Or if you prefer install the operator's resources with SSL disabled:

    helm install --name flink-k8s-toolbox-operator --namespace flink helm/flink-k8s-toolbox-operator

Run the operator with command:

    kubectl -n flink scale deployment flink-operator --replicas=1

## Uninstall Flink Operator

Stop the operator with command:

    kubectl -n flink scale deployment flink-operator --replicas=0

Remove the operator's resources with command:    

    helm delete --purge flink-k8s-toolbox-operator

Remove the operator's CRD resource with command:    

    helm delete --purge flink-k8s-toolbox-crd

Remove secret with command:    

    kubectl -n flink delete secret flink-operator-ssl

Remove namespace with command:    

    helm delete namespace flink

## Upgrade Flink Operator from previous version

PLEASE NOTE THAT THE OPERATOR IS STILL IN BETA VERSION AND IT DOESN'T HAVE A STABLE API YET, THEREFORE EACH RELEASE MIGHT INTRODUCE BREAKING CHANGES.

Before upgrading to a new release you must cancel all jobs creating a savepoint into a durable storage location (for instance AWS S3).

Create a copy of your FlinkCluster resources:

    kubectl -n flink get fc -o yaml > clusters-backup.yaml

Upgrade the CRD using Helm:

    helm upgrade flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd

Upgrade the operator using Helm:

    helm upgrade flink-k8s-toolbox-operator --namespace flink helm/flink-k8s-toolbox-operator --set secretName=flink-operator-ssl

After installing the new version, you can restart the jobs. However, the custom resources might not be compatible with the new CRD.
If that is the case, then you have to fix the resource specification, perhaps you have to delete the resource and recreate it.

## Get Docker image of Flink Operator

The operator's Docker image can be downloaded from Docker Hub:

    docker fetch nextbreakpoint/flink-k8s-toolbox:1.2.1-beta

Tag and push the image into your private registry if needed:

    docker tag nextbreakpoint/flink-k8s-toolbox:1.2.1-beta some-registry/flink-k8s-toolbox:1.2.1-beta
    docker login some-registry
    docker push some-registry/flink-k8s-toolbox:1.2.1-beta

## Run Flink Operator manually

Run the operator using the image on Docker Hub:

    kubectl run flink-operator --restart=Never -n flink --image=nextbreakpoint/flink-k8s-toolbox:1.2.1-beta \
        --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always" } }' -- operator run --namespace=flink

Or run the operator using your private registry and pull secrets:

    kubectl run flink-operator --restart=Never -n flink --image=some-registry/flink-k8s-toolbox:1.2.1-beta \
        --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always", "imagePullSecrets": [{"name": "your-pull-secrets"}] } }' -- operator run --namespace=flink

Please note that you **MUST** run only one operator for each namespace to avoid conflicts.

A service account is created when installing the operator Helm chart:

    helm install --name flink-k8s-toolbox-operator --namespace flink helm/flink-k8s-toolbox-operator

Verify that the pod has been created:

    kubectl -n flink get pod flink-operator -o yaml     

Verify that there are no errors in the logs:

    kubectl -n flink logs flink-operator

Check the events in case that the pod doesn't start:

    kubectl -n flink get events

Stop the operator with command:

    kubectl -n flink delete pod flink-operator

## Flink operator custom resources

Flink Operator requires a Custom Resource Definition:

    apiVersion: apiextensions.k8s.io/v1beta1
    kind: CustomResourceDefinition
    metadata:
      name: flinkclusters.nextbreakpoint.com
    spec:
      preserveUnknownField: false
      group: nextbreakpoint.com
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
    [...]

FlinkCluster resources can be created or deleted as any other resource in Kubernetes using kubectl command.

The Custom Resource Definition is installed with a separate Helm chart:

    helm install --name flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd

The complete definition with the validation schema is defined in the Helm template:

    https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/helm/flink-k8s-toolbox-crd/templates/crd.yaml

Do not delete the CRD unless you want to delete all custom resources depending on it.

When updating the operator, upgrade the CRD with Helm instead of deleting and reinstalling:

    helm upgrade flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd  

### Create a Flink cluster

Make sure the CRD has been installed (see above).

Create a Docker file:

    FROM nextbreakpoint/flink-k8s-toolbox:1.2.1-beta
    COPY flink-jobs.jar /flink-jobs.jar

where flink-jobs.jar contains the code of your Flink jobs.

Create a Docker image:

    docker build -t flink-jobs:1 .

Tag and push the image into your registry if needed:

    docker tag flink-jobs:1 some-registry/flink-jobs:1
    docker login some-registry
    docker push some-registry/flink-jobs:1

Pull Flink image:

    docker pull flink:1.9.0

Tag and push the image into your registry if needed:

    docker tag flink:1.9.0 some-registry/flink:1.9.0
    docker login some-registry
    docker push some-registry/flink:1.9.0

Create a Flink Cluster file:

    cat <<EOF >test.yaml
    apiVersion: "nextbreakpoint.com/v1"
    kind: FlinkCluster
    metadata:
      name: test
    spec:
      taskManagers: 1
      runtime:
        pullPolicy: Always
        image: some-registry/flink:1.9.0
      bootstrap:
        pullPolicy: Always
        image: some-registry/flink-jobs:1
        jarPath: /flink-jobs.jar
        className: com.nextbreakpoint.flink.jobs.TestJob
        arguments:
          - --DEVELOP_MODE
          - disabled
      jobManager:
        serviceMode: NodePort
        annotations:
          managed: true
        environment:
        - name: FLINK_GRAPHITE_HOST
          value: graphite.default.svc.cluster.local
        environmentFrom:
        - secretRef:
            name: flink-secrets
        volumeMounts:
          - name: jobmanager
            mountPath: /var/tmp
        extraPorts:
          - name: prometheus
            containerPort: 9999
            protocol: TCP
        persistentVolumeClaimsTemplates:
          - metadata:
              name: jobmanager
            spec:
              storageClassName: hostpath
              accessModes:
               - ReadWriteOnce
              resources:
                requests:
                  storage: 1Gi
      taskManager:
        taskSlots: 1
        annotations:
          managed: true
        environment:
        - name: FLINK_GRAPHITE_HOST
          value: graphite.default.svc.cluster.local
        volumeMounts:
          - name: taskmanager
            mountPath: /var/tmp
        extraPorts:
          - name: prometheus
            containerPort: 9999
            protocol: TCP
        persistentVolumeClaimsTemplates:
          - metadata:
              name: taskmanager
            spec:
              storageClassName: hostpath
              accessModes:
               - ReadWriteOnce
              resources:
                requests:
                  storage: 5Gi
      operator:
        # savepointMode can be Automatic or Manual. Default value is Manual
        savepointMode: Automatic
        # savepointInterval in seconds. Required when savepointMode is Automatic
        savepointInterval: 60
        # savepointTargetPath can be any valid Hadoop filesystem (including S3)
        savepointTargetPath: file:///var/tmp/test
        # jobRestartPolicy can be Always or Never. Default value is Never
        jobRestartPolicy: Always
    EOF

Create a FlinkCluster resource with command:

    kubectl create -n flink -f test.yaml

Please note that you can use any image of Flink as far as the image implements the standard commands for running JobManager and TaskManager.

### Delete a Flink cluster

Delete a FlinkCluster with command:

    kubectl delete -n flink -f test.yaml

### List Flink clusters

List custom objects of type FlinkCluster with command:

    kubectl get -n flink flinkclusters

The command should produce an output like:

    NAME   CLUSTER-STATUS   TASK-STATUS   TASK             TASK-MANAGERS   TASK-SLOTS   ACTIVE-TASK-MANAGERS   TOTAL-TASK-SLOTS   JOB-PARALLELISM   JOB-RESTART   SERVICE-MODE   SAVEPOINT-MODE   SAVEPOINT-PATH                                       SAVEPOINT-AGE   AGE
    test   Running          Idle          ClusterRunning   1               1            1                      1                  1                 Always        NodePort       Manual           file:/var/savepoints/savepoint-e0e430-7a6d1c33dee3   42s             3m55s

## Build Flink Operator from source code

Build the uber JAR file with command:

    ./gradlew clean shadowJar

and test the JAR printing the CLI usage:

    java -jar build/libs/flink-k8s-toolbox-1.2.1-beta-with-dependencies.jar --help

Build a Docker image with command:

    docker build -t flink-k8s-toolbox:1.2.1-beta .

and test the image printing the CLI usage:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta --help

Tag and push the image to your Docker registry if needed:

    docker tag flink-k8s-toolbox:1.2.1-beta some-registry/flink-k8s-toolbox:1.2.1-beta
    docker login some-registry
    docker push some-registry/flink-k8s-toolbox:1.2.1-beta

## Automatic savepoints

The operator automatically creates savepoints before stopping the cluster.
This might happen when a change is applied to the cluster specification or the cluster is rescaled or manually stopped.
This feature is very handy to avoid losing the status of the job.
When the operator restarts the cluster, it uses the latest savepoint to recover the status of the job.
However, for this feature to work properly, the savepoints must be created in a durable storage location such as HDFS or S3.
Only a durable location can be used to recover the job after recreating the Job Manager and the Task Managers.

## How to use the Operator CLI

Print the CLI usage:

    docker run --rm -it nextbreakpoint/flink-k8s-toolbox:1.2.1-beta --help

The output should look like:

    Usage: flink-k8s-toolbox [OPTIONS] COMMAND [ARGS]...

    Options:
      -h, --help  Show this message and exit

    Commands:
      operator      Access operator subcommands
      cluster       Access cluster subcommands
      savepoint     Access savepoint subcommands
      bootstrap     Access bootstrap subcommands
      job           Access job subcommands
      jobmanager    Access JobManager subcommands
      taskmanager   Access TaskManager subcommands
      taskmanagers  Access TaskManagers subcommands

### How to create a cluster

Create a Docker file like:

    FROM nextbreakpoint/flink-k8s-toolbox:1.2.1-beta
    COPY flink-jobs.jar /flink-jobs.jar

where flink-jobs.jar contains the code of your Flink job.

Create a Docker image:

    docker build -t flink-jobs:1 .

Tag and push the image into your registry if needed:

    docker tag flink-jobs:1 some-registry/flink-jobs:1
    docker login some-registry
    docker push some-registry/flink-jobs:1

Create a JSON file:

    cat <<EOF >test.json
    {
      "taskManagers": 1,
      "runtime": {
        "pullPolicy": "Always",
        "image": "some-registry/flink:1.9.0"
      },
      "bootstrap": {
        "pullPolicy": "Always",
        "image": "some-registry/flink-jobs:1",
        "jarPath": "/flink-jobs.jar",
        "className": "com.nextbreakpoint.flink.jobs.TestJob",
        "arguments": [
          "--DEVELOP_MODE",
          "disabled"
        ]
      },
      "jobManager": {
        "serviceMode": "NodePort",
        "environment": [
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
        "volumeMounts": [
          {
            "name": "jobmanager",
            "mountPath": "/var/tmp"
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
        ],
        "persistentVolumeClaimsTemplates": [
          {
            "metadata": {
              "name": "jobmanager"
            },
            "spec": {
              "storageClassName": "hostpath",
              "accessModes": [ "ReadWriteOnce" ],
              "resources": {
                "requests": {
                  "storage": "1Gi"
                }
              }
            }
          }
        ]
      },
      "taskManager": {
        "environment": [
          {
            "name": "FLINK_GRAPHITE_HOST",
            "value": "graphite.default.svc.cluster.local"
          }
        ],
        "volumeMounts": [
          {
            "name": "taskmanager",
            "mountPath": "/var/tmp"
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
        ],
        "persistentVolumeClaimsTemplates": [
          {
            "metadata": {
              "name": "taskmanager"
            },
            "spec": {
              "storageClassName": "hostpath",
              "accessModes": [ "ReadWriteOnce" ],
              "resources": {
                "requests": {
                  "storage": "5Gi"
                }
              }
            }
          }
        ]
      },
      "operator": {
        "savepointMode": "Automatic",
        "savepointInterval": 60,
        "savepointTargetPath": "file:///var/tmp/test",
        "jobRestartPolicy": "OnFailure"
      }
    }
    EOF

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster create --cluster-name=test --cluster-spec=test.json --host=$OPERATOR_HOST --port=4444

Pass keystore and truststore if SSL is enabled:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster create --cluster-name=test --cluster-spec=test.json --host=$OPERATOR_HOST --port=4444
    --keystore-path=secrets/keystore-operator-cli.jks --truststore-path=secrets/truststore-operator-cli.jks --keystore-secret=keystore-password --truststore-secret=truststore-password

If you expose the operator on a port of Docker's host:

        Set OPERATOR_HOST to localhost on Linux

        Set OPERATOR_HOST to host.docker.internal on MacOS

Show more options with the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster create --help

### How to get the status of a cluster

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster status --cluster-name=test --host=$OPERATOR_HOST --port=4444

Use grep and jq to format the output:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster status --cluster-name=test --host=$OPERATOR_HOST --port=4444 | grep -v WARNING | jq -r

Show more options with the command:

     docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster status --help

### How to delete a cluster

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster delete --cluster-name=test --host=$OPERATOR_HOST --port=4444

Show more options with the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster delete --help

### How to stop a running cluster

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster stop --cluster-name=test --host=$OPERATOR_HOST --port=4444

Show more options with the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster stop --help

### How to restart a stopped or failed cluster

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster start --cluster-name=test --host=$OPERATOR_HOST --port=4444

Show more options with the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster start --help

### How to start a cluster and run the job without savepoint

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster start --cluster-name=test --without-savepoint --host=$OPERATOR_HOST --port=4444

### How to stop a cluster without creating a savepoint

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster stop --cluster-name=test --without-savepoint --host=$OPERATOR_HOST --port=4444

### How to create a new savepoint

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta savepoint trigger --cluster-name=test --host=$OPERATOR_HOST --port=4444

### How to get the status of a cluster

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster status --cluster-name=test --host=$OPERATOR_HOST --port=4444

### How to scale a cluster

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta cluster scale --cluster-name=test --task-managers=4 --host=$OPERATOR_HOST --port=4444

### How to get the details of the running job

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta job details --cluster-name=test --host=$OPERATOR_HOST --port=4444

### How to get the metrics of the running job

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta job metrics --cluster-name=test --host=$OPERATOR_HOST --port=4444

### How to get a list of Task Managers

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta taskmanagers list --cluster-name=test --host=$OPERATOR_HOST --port=4444

### How to get the metrics of the Job Manager

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta jobmanager metrics --cluster-name=test --host=$OPERATOR_HOST --port=4444

### How to get the metrics of a Task Manager

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta taskmanager metrics --cluster-name=test --host=$OPERATOR_HOST --port=4444

You will be asked to provide a Task Manager id which you can get from the list of Task Managers.   

### How to get the details of a Task Manager

Execute the command:

    docker run --rm -it flink-k8s-toolbox:1.2.1-beta taskmanager details --cluster-name=test --host=$OPERATOR_HOST --port=4444

You will be asked to provide a Task Manager id which you can get from the list of Task Managers.   

### How to upload a JAR file and start a job

Flink jobs must be packaged in a regular JAR file and uploaded to the JobManager.

Upload a JAR file using the command:

    java -jar flink-k8s-toolbox-1.2.1-beta.jar bootstrap run --cluster-name=test --class-name=your-main-class --jar-path=/your-job-jar.jar

When running outside Kubernetes use the command:

    java -jar flink-k8s-toolbox-1.2.1-beta.jar bootstrap run --kube-config=/your-kube-config --cluster-name=test --class-name=your-main-class --jar-path=/your-job-jar.jar

### How to run the Operator for testing

The Flink Operator can be executed as Docker image or JAR file, pointing to a local or remote Kubernetes cluster.    

Run the operator with a given namespace and Kubernetes config using the JAR file:

    java -jar flink-k8s-toolbox:1.2.1-beta.jar operator run --namespace=test --kube-config=~/.kube/config

Run the operator with a given namespace and Kubernetes config using the Docker image:

    docker run --rm -it -v ~/.kube/config:/kube/config flink-k8s-toolbox:1.2.1-beta operator run --namespace=test --kube-config=/kube/config
