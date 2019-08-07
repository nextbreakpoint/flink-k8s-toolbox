# Flink Kubernetes Toolbox

Flink K8S Toolbox contains tools for managing Flink clusters and jobs on Kubernetes:

- Flink Operator
- Flink Operator CLI

The Flink Operator is an implementation of the Kubernetes Operator pattern for managing Flink clusters and jobs.
The operator uses a Custom Resource to represent a Flink cluster with a single Flink job.
The operator detect changes to the resource and modifies the Flink cluster and job accordingly.
The operator takes care of creating savepoints periodically and restarting the job from the latest savepoint if needed.

The Flink Operator CLI provides an interface for controlling Flink clusters and jobs from a terminal. 
It supports commands for creating or deleting clusters, starting or stopping clusters and jobs, 
getting metrics and other information about clusters and jobs. 

*Flink Operator supports only long-running stream jobs. Batch jobs are not supported at the moment.*           

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
resources (secondary resources), like StatefulSet, Service, and Job, based on the configuration provided in the primary resource.  
The operator persists some status as annotations on the primary resources, and performs several tasks automatically, 
like creating savepoints periodically or recreating the cluster when the primary resource is modified. 

The possible states of a FlinkCluster resource are represented in this graph:

![Flink cluster status graph](/graphs/flink-cluster-status.dot.png "Flink cluster status graph")

- **UNKNOWN** 
  
  This is the initial status of the Flink cluster when it is created.

- **STARTING**

  This status means that the cluster is starting up. The secondary resources will be created, the Flink job Jar will be uploaded to Flink, and the job will be started (optional).

- **STOPPING** 

  This status means that the cluster is going to stop. The Flink job will be canceled (creating a new savepoint) or stopped (without creating a new savepoint), and the secondary resources will be deleted (optional).

- **SUSPENDED** 

  This status means that the cluster has been suspended. The Flink job has been stopped, and the secondary resources have been stopped.

- **TERMINATED** 

  This status means that the cluster has been terminated. The Flink job has been stopped, and the secondary resources have been deleted.

- **RUNNING** 

  This status means that the cluster is running. The secondary resources have been created and the Flink job is runnning. When the cluster is in this state and the job stops running for any reason, the status is automatically changed to FAILED.

- **FAILED** 

  This status means that the cluster has failed. The secondary resources might not work or the job might have failed (or it has been canceled manually). When the cluster is in this state but a running job is detected, the status is automatically changed to RUNNING.

- **CHECKPOINTING** 
  
  This status means that the cluster is creating a savepoint. The status is automatically changed to RUNNING when the savepoint is completed, otherwise the status is changed to FAILED (when the savepoint is not completed in the expected time).

Each arrow in this graph represents a specific sequence of tasks which are executed in order to transition from a status to another. Each task of the sequence is processed according to this graph:

![Task executor status graph](/graphs/task-executor-status.dot.png "Task executor status graph")

- **EXECUTING** 
  
  This is the initial status of a task. The task is executing some operation against Kubernetes resources or Flink server. The task might repeat the operation several times before succeeding or timing out and generating a failure.

- **AWAITING** 
  
  The task executed some operation and it is now awaiting for some results. The task might check for results several times before succeeding or timing out and generating a failure.

- **IDLE** 
  
  The task found the expected results and it is now completed. The task is idle but it keeps checking for new tasks to execute and it can eventually schedule a new task. 

- **FAILED** 
  
  The task didn't complete and some errors occurred. The cluster status is changed to FAILED and the DO_NOTHING task is scheduled for execution. 

The possible tasks which are executed to transition from one status to another are:

- **INITIALISE_CLUSTER** 

  Initialise primary resource and change cluster status to starting.    
  
- **HALT_CLUSTER** 

  Detect changes in the primary resource and restart cluster if needed. Detect running job and change cluster status to RUNNING.    
  
- **RUN_CLUSTER** 

  Detect changes in the primary resource and restart cluster if needed. Detect failed job and change cluster status to FAILED. Periodically change cluster status to CHECKPOINTING to create a new savepoint.     

- **STARTING_CLUSTER** 

  Set cluster status to STARTING.
       
- **STOPPING_CLUSTER** 

  Set cluster status to STOPPING.
  
- **CHECKPOINTING_CLUSTER** 

  Set cluster status to CHECKPOINTING.
  
- **CREATE_RESOURCES** 

  Create secondary resources and wait until resources reach expected status.
  
- **DELETE_RESOURCES** 

  Delete secondary resources and wait until resources reach expected status.
  
- **DELETE_UPLOAD_JOB** 

  Remove batch job previously used to upload JAR file.
  
- **TERMINATE_PODS** 

  Terminate pods scaling down resources (set replicas to 0). 
  
- **RESTART_PODS** 

  Restart pods scaling up resources (set replicas to expected value). 
  
- **UPLOAD_JAR** 

  Schedule batch job which upload JAR file to Flink.  
  
- **CANCEL_JOB** 

  Cancel job creating a new savepoint and wait until savepoint is completed.  
  
- **START_JOB** 

  Start job using configuration from primary resource. Restart job from savepoint when savepoint path is available in primary resource.   
  
- **STOP_JOB** 

  Cancel job without creating a savepoint.  
  
- **CREATE_SAVEPOINT** 

  Trigger new savepoint and wait until savepoint is completed. 
  
- **ERASE_SAVEPOINT** 

  Remove savepoint from primary resource. 

## Flink Operator REST API

The operator has a REST API which is exposed on port 4444 by default. The API provides information about the status of the resources, metrics of the clusters and jobs, and more:

    http://localhost:4444/cluster/<name>/status
    
    http://localhost:4444/cluster/<name>/job/details
    
    http://localhost:4444/cluster/<name>/job/metrics
    
    http://localhost:4444/cluster/<name>/jobmanager/metrics
    
    http://localhost:4444/cluster/<name>/taskmanagers
    
    http://localhost:4444/cluster/<name>/taskmanagers/<taskmanager>/metrics

Please note that you must use SSL certificates when HTTPS is enabled (see instructions for generating SSL certificates):

    curl --cacert secrets/ca_cert.pem --cert secrets/operator-cli_cert.pem --key secrets/operator-cli_key.pem https://localhost:4444/cluster/test/status 

## Generate SSL certificates and keystores

Execute the script secrets.sh to generate self-signed certificates and keystores required to run the Flink Operator.

    ./secrets.sh flink-operator key-password keystore-password truststore-password

This command will generate new certificates and keystores in the directory secrets. 

## Monitoring Flink Operator

The operator exposes metrics to Prometheus on port 8080 by default. The metrics are exposed using HTTP protocol.

    http://localhost:8080/metrics
     
## Install Flink Operator

Create a namespace with command:

    kubectl create namespace flink

Create a secret which contain the keystore and the truststore files:

    kubectl create secret generic flink-operator-ssl -n flink --from-file=keystore.jks=secrets/keystore-operator-api.jks --from-file=truststore.jks=secrets/truststore-operator-api.jks --from-literal=keystore-secret=keystore-password --from-literal=truststore-secret=truststore-password 

Install the operator global resources with commands:

    helm install --name flink-k8s-toolbox-global helm/flink-k8s-toolbox-global

Install the operator namespaced resources with commands:

    helm install --name flink-k8s-toolbox-services --namespace flink helm/flink-k8s-toolbox-services --set ssl.secretName=flink-operator-ssl  

## Uninstall Flink Operator

Remove the operator resources with commands:    

    helm delete --purge flink-k8s-toolbox-services
    helm delete --purge flink-k8s-toolbox-global

Remove secret with command:    

    kubectl delete secret -n flink flink-operator-ssl 

Remove namespace with command:    

    helm delete namespace flink

## Get Docker image of Flink Operator

The Docker image can be downloaded from Docker Hub:

    docker fetch nextbreakpoint/flink-k8s-toolbox:1.1.4-beta

Tag and push the image into your registry if needed:

    docker tag nextbreakpoint/flink-k8s-toolbox:1.1.4-beta some-registry/flink-k8s-toolbox:1.1.4-beta
    docker login some-registry
    docker push some-registry/flink-k8s-toolbox:1.1.4-beta

## Run Flink Operator manually

Run the operator using the image on Docker Hub:

    kubectl run flink-operator --restart=Never -n flink --image=nextbreakpoint/flink-k8s-toolbox:1.1.4-beta --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always" } }' -- operator run --namespace=flink

Or run the operator using your own registry and pull secrets:

    kubectl run flink-operator --restart=Never -n flink --image=some-registry/flink-k8s-toolbox:1.1.4-beta --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always", "imagePullSecrets": [{"name": "your-pull-secrets"}] } }' -- operator run --namespace=flink

Please note that you **MUST** run only one operator for each namespace to avoid conflicts.

Verify that the pod has been created:

    kubectl get pod flink-operator -o yaml     

Verify that there are no errors in the logs:

    kubectl logs flink-operator

Check the system events if the pod doesn't start:

    kubectl get events

## Custom resources 

Flink Operator requires a Custom Resource Definition:

    apiVersion: apiextensions.k8s.io/v1beta1
    kind: CustomResourceDefinition
    metadata:
      name: flinkclusters.nextbreakpoint.com
    spec:
      group: nextbreakpoint.com
      version: "v1"
      scope: Namespaced
      names:
        plural: flinkclusters
        singular: flinkcluster
        kind: FlinkCluster
        shortNames:
        - fc

FlinkCluster resources can be created or deleted as any other resource in Kubernetes using kubectl command.

The Custom Resource Definition is installed with the Helm chart. 

### Create a Flink cluster 

Make sure the CRD has been installed (see above).

Create a Docker file like:

    FROM nextbreakpoint/flink-k8s-toolbox:1.1.4-beta
    COPY flink-jobs.jar /flink-jobs.jar

where flink-jobs.jar contains the code of your Flink jobs.

Create a Docker image:

    docker build -t flink-jobs:1 .

Tag and push the image into your registry if needed:

    docker tag flink-jobs:1 some-registry/flink-jobs:1
    docker login some-registry
    docker push some-registry/flink-jobs:1

Create a ConfigMap file:

    cat <<EOF >config-map.yaml
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: flink-config
    data:
      core-site.xml: |
        <?xml version="1.0" encoding="UTF-8"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration>
        <property>
        <name>fs.s3.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
        </property>
        <property>
        <name>fs.s3a.buffer.dir</name>
        <value>/tmp</value>
        </property>
        <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>com.amazonaws.auth.EnvironmentVariableCredentialsProvider</value>
        </property>
        </configuration>
    EOF

Create a ConfigMap resource with command:

    kubectl create -n flink -f config-map.yaml

Create a FlinkCluster file:

    cat <<EOF >flink-cluster-test.yaml
    apiVersion: "nextbreakpoint.com/v1"
    kind: FlinkCluster
    metadata:
      name: test
    spec:
      flinkImage:
        pullPolicy: IfNotPresent
        flinkImage: nextbreakpoint/flink:1.7.2-1
      flinkJob:
        image: flink-jobs:1
        jarPath: /flink-jobs.jar
        className: com.nextbreakpoint.flink.jobs.TestJob
        parallelism: 1
        arguments:
          - --BUCKET_BASE_PATH
          - file:///var/tmp
      jobManager:
        serviceMode: NodePort
        environment:
        - name: FLINK_GRAPHITE_HOST
          value: graphite.default.svc.cluster.local
        volumeMounts:
          - name: config-vol
            mountPath: /hadoop/etc/core-site.xml
            subPath: core-site.xml
        volumes:
          - name: config-vol
            configMap:
              name: flink-config
      taskManager:
        environment:
        - name: FLINK_GRAPHITE_HOST
          value: graphite.default.svc.cluster.local
        volumeMounts:
          - name: config-vol
            mountPath: /hadoop/etc/core-site.xml
            subPath: core-site.xml
        volumes:
          - name: config-vol
            configMap:
              name: flink-config
      flinkOperator:
        targetPath: file:///var/tmp/test
    EOF

Create a FlinkCluster resource with command:

    kubectl create -n flink -f flink-cluster-test.yaml

Please note that you can use any image of Flink as far as the image uses the standard commands to run JobManager and TaskManager. 

### Delete a Flink cluster

Delete the custom object with command:

    kubectl delete -n flink -f flink-cluster-test.yaml

### List Flink clusters

List all custom objects with command:

    kubectl get -n flink flinkclusters

## Build Flink Operator from source code

Create a file toolchains.xml:

    <?xml version="1.0" encoding="UTF8"?>
    <toolchains>
      <toolchain>
        <type>jdk</type>
        <provides>
          <version>11</version>
          <vendor>adoptjdk</vendor>
        </provides>
        <configuration>
          <jdkHome>/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home</jdkHome>
        </configuration>
      </toolchain>
    </toolchains>

Create fat JAR and Docker image using Maven:

    mvn -t toolchains.xml clean package -Dbuild-docker-image=true

Tag and push the image to your Docker registry if needed:

    docker tag flink-k8s-toolbox:1.1.4-beta some-registry/flink-k8s-toolbox:1.1.4-beta
    docker login some-registry
    docker push some-registry/flink-k8s-toolbox:1.1.4-beta

## How to use the CLI tool

CLI commands can be executed as Docker image or as JAR file.   

For instance you can show the command usage using the JAR file:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar --help

Or you can show the command usage using the Docker image:

    docker run --rm -it nextbreakpoint/flink-k8s-toolbox:1.1.4-beta --help

The output should look like:

    Usage: flink-k8s-toolbox [OPTIONS] COMMAND [ARGS]...
    
    Options:
      -h, --help  Show this message and exit
    
    Commands:
      operator      Access operator subcommands
      cluster       Access cluster subcommands
      upload        Access upload subcommands
      job           Access job subcommands
      jobmanager    Access JobManager subcommands
      taskmanager   Access TaskManager subcommands
      taskmanagers  Access TaskManagers subcommands

### How to create a cluster

Create a Docker file like:

    FROM nextbreakpoint/flink-k8s-toolbox:1.1.4-beta
    COPY flink-jobs.jar /flink-jobs.jar

where flink-jobs.jar contains the code of your Flink jobs.

Create a Docker image:

    docker build -t flink-jobs:1 .

Tag and push the image into your registry if needed:

    docker tag flink-jobs:1 some-registry/flink-jobs:1
    docker login some-registry
    docker push some-registry/flink-jobs:1

Create a JSON file:

    cat <<EOF >flink-cluster-test.json
    {
      "flinkImage": {
        "pullPolicy": "IfNotPresent",
        "flinkImage": "nextbreakpoint/flink:1.7.2-1"
      },
      "flinkJob": {
        "image": "flink-jobs:1",
        "jarPath": "/flink-jobs.jar",
        "className": "com.nextbreakpoint.flink.jobs.TestJob",
        "parallelism": 1,
        "arguments": [
          "--BUCKET_BASE_PATH",
          "file:///var/tmp"
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
        "volumeMounts": [
          {
            "name": "config-vol",
            "mountPath": "/hadoop/etc/core-site.xml",
            "subPath": "core-site.xml"
          }
        ],
        "volumes": [
          {
            "name": "config-vol",
            "configMap": {
              "name": "flink-config"
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
            "name": "config-vol",
            "mountPath": "/hadoop/etc/core-site.xml",
            "subPath": "core-site.xml"
          }
        ],
        "volumes": [
          {
            "name": "config-vol",
            "configMap": {
              "name": "flink-config"
            }
          }
        ]
      },
      "flinkOperator": {
        "targetPath": "file:///var/tmp/test"
      }
    }
    EOF

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar \
        cluster \
        create \
        --cluster-name=test \
        --cluster-spec=flink-cluster-test.json 

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar cluster create --help

### How to delete a cluster

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar \
        cluster \
        delete \
        --cluster-name=test

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar cluster delete --help

### How to stop a running cluster

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar \
        cluster \
        stop \
        --cluster-name=test
        --with-savepoint

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar cluster stop --help

### How to start a stopped cluster

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar \
        cluster \
        start \
        --cluster-name=test

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar cluster start --help

### How to start a stopped cluster but don't run the job

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar \
        cluster \
        start \
        --cluster-name=test
        --start-only-cluster

### How to start a stopped cluster and run the job without savepoint

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar \
        cluster \
        start \
        --cluster-name=test
        --without-savepoint

### How to stop a running cluster without creating a savepoint

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar \
        cluster \
        stop \
        --cluster-name=test

### How to upload the JAR file

Flink jobs must be packaged in a regular JAR file.

Upload the JAR with command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar upload jar --cluster-name=test --class-name=your-main-class --jar-path=/your-job-jar.jar

When running outside Kubernetes use the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar upload jar --kube-config=/your-kube-config.conf --cluster-name=test --class-name=your-main-class --jar-path=/your-job-jar.jar

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.4-beta.jar upload jar --help

### How to run the Operator for testing

The Flink operator can be executed as Docker image or as JAR file.   

Run the operator with a given namespace and Kubernetes config using the JAR file:

    java -jar com.nextbreakpoint.flink-k8s-toolbox:1.1.4-beta.jar operator run --namespace=test --kube-config=/path/admin.conf

Or run the operator with a given namespace and Kubernetes config using the Docker image:

    docker run --rm -it -v /path/admin.conf:/admin.conf flink-k8s-toolbox:1.1.4-beta operator run --namespace=test --kube-config=/admin.conf
