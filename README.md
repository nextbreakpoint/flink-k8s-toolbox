# Flink Kubernetes Toolbox

Flink K8S Toolbox contains tools for managing Flink clusters on Kubernetes.

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

## Install with Helm

Create a namespace with command:

    kubectl create namespace flink

Install CRD and cluster roles with command:

    helm install --name flink-k8s-toolbox-global charts/flink-k8s-toolbox-global

Install service accounts in namespace with command:

    helm install --name flink-k8s-toolbox-accounts --namespace flink charts/flink-k8s-toolbox-accounts

Install operator services in namespace with command:

    helm install --name flink-k8s-toolbox-services --namespace flink charts/flink-k8s-toolbox-services

## Uninstall with Helm

Remove operator services with command:    

    helm delete --purge flink-k8s-toolbox-services

Remove service accounts with command:    

    helm delete --purge flink-k8s-toolbox-accounts

Remove CRD and cluster roles with command:    

    helm delete --purge flink-k8s-toolbox-global

Remove namespace with command:    

    helm delete namespace flink

## Get Docker image

The Docker image can be downloaded from Docker Hub:

    docker fetch nextbreakpoint/flink-k8s-toolbox:1.1.1-beta

Tag and push the image into your registry:

    docker tag nextbreakpoint/flink-k8s-toolbox:1.1.1-beta some-registry/flink-k8s-toolbox:1.1.1-beta

    docker login some-registry

    docker push some-registry/flink-k8s-toolbox:1.1.1-beta

## Run Flink Operator manually

Run the operator using the image on Docker Hub:

    kubectl run flink-operator --restart=Never --image=nextbreakpoint/flink-k8s-toolbox:1.1.1-beta --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always" } }' -- operator run --namespace=test

Or run the operator using your own registry and pull secrets:

    kubectl run flink-operator --restart=Never --image=some-registry/flink-k8s-toolbox:1.1.1-beta --overrides='{ "apiVersion": "v1", "metadata": { "labels": { "app": "flink-operator" } }, "spec": { "serviceAccountName": "flink-operator", "imagePullPolicy": "Always", "imagePullSecrets": [{"name": "your-pull-secrets"}] } }' -- operator run --namespace=test

Please note that you must run only one operator for each namespace to avoid conflicts.

Verify that the pod has been created:

    kubectl get pod flink-operator -o yaml     

The pod should have a label app with value *flink-operator* and it should run with *flink-operator* service account.

Verify that there are no errors in the logs:

    kubectl logs flink-operator

Check the system events if the pod doesn't start:

    kubectl get events

## Custom Resource Definition

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

The Custom Resource Definition is installed with the Helm chart. 

## How to create a Flink Cluster using CRD

Flink Clusters can be created or deleted as any other resource in Kubernetes.

### Create a Flink Cluster 

Make sure the CRD has been installed (see above).

Create a Docker file like:

    FROM nextbreakpoint/flink-k8s-toolbox:1.1.1-beta
    COPY flink-jobs.jar /flink-jobs.jar

where flink-jobs.jar contains the code of your Flink jobs.

Create a Docker image:

    docker build -t flink-jobs:1 .

Tag and push the image into your registry if required:

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

    kubectl create -f config-map.yaml

Create a FlinkCluster file:

    cat <<EOF >flink-cluster-test.yaml
    apiVersion: "nextbreakpoint.com/v1"
    kind: FlinkCluster
    metadata:
      name: test
    spec:
      flinkImage:
        pullSecrets: regcred
        pullPolicy: IfNotPresent
        flinkImage: registry:30000/flink:1.7.2
      flinkJob:
        image: registry:30000/flink-jobs:1
        jarPath: /flink-jobs.jar
        className: com.nextbreakpoint.flink.jobs.TestJob
        parallelism: 1
        arguments:
          - --BUCKET_BASE_PATH
          - file:///var/tmp
      jobManager:
        serviceMode: NodePort
        storageClass: hostpath
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
        serviceMode: NodePort
        storageClass: hostpath
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

    kubectl create -f flink-cluster-test.yaml

Please note you can use other images of Flink. 

### Delete FlinkCluster resource

Delete the custom object with command:

    kubectl delete -f flink-cluster-test.yaml

### List FlinkCluster resources

List all custom objects with command:

    kubectl get flinkclusters

## Build from source code

Configure your toolchains (~/.m2/toolchains.xml):

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

    mvn clean package

Tag and push the image to your Docker registry:

    docker tag flink-k8s-toolbox:1.1.1-beta some-registry/flink-k8s-toolbox:1.1.1-beta

    docker login some-registry

    docker push some-registry/flink-k8s-toolbox:1.1.1-beta

## How to use the CLI tool

CLI commands can be executed as Docker image or as JAR file.   

For instance you can show the command usage using the JAR file:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar --help

Or you can show the command usage using the Docker image:

    docker run --rm -it nextbreakpoint/flink-k8s-toolbox:1.1.1-beta --help

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

    FROM nextbreakpoint/flink-k8s-toolbox:1.1.1-beta
    COPY flink-jobs.jar /flink-jobs.jar

where flink-jobs.jar contains the code of your Flink jobs.

Create a Docker image:

    docker build -t flink-jobs:1 .

Tag and push the image into your registry:

    docker tag flink-jobs:1 some-registry/flink-jobs:1

    docker login some-registry

    docker push some-registry/flink-jobs:1

Create a JSON file:

    cat <<EOF >flink-cluster-test.json
    {
      "flinkImage": {
        "pullSecrets": "regcred",
        "pullPolicy": "IfNotPresent",
        "flinkImage": "registry:30000/flink:1.7.2"
      },
      "flinkJob": {
        "image": "registry:30000/flink-jobs:1",
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
        "storageClass": "hostpath",
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
        "serviceMode": "NodePort",
        "storageClass": "hostpath",
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

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar \
        cluster \
        create \
        --cluster-name=test \
        --cluster-spec=flink-cluster-test.json 

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar cluster create --help

### How to delete a cluster

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar \
        cluster \
        delete \
        --cluster-name=test

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar cluster delete --help

### How to stop a running cluster

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar \
        cluster \
        stop \
        --cluster-name=test
        --with-savepoint

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar cluster stop --help

### How to start a stopped cluster

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar \
        cluster \
        start \
        --cluster-name=test

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar cluster start --help

### How to start a stopped cluster but don't run the job

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar \
        cluster \
        start \
        --cluster-name=test
        --start-only-cluster

### How to start a stopped cluster and run the job without savepoint

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar \
        cluster \
        start \
        --cluster-name=test
        --without-savepoint

### How to stop a running cluster without creating a savepoint

Execute the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar \
        cluster \
        stop \
        --cluster-name=test

### How to upload the JAR file

Flink jobs must be packaged in a regular JAR file.

Upload the JAR with command when running within Kubernetes:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar upload jar --cluster-name=test --class-name=your-main-class --jar-path=/your-job-jar.jar

Or upload the JAR with command when running outside Kubernetes:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar upload jar --kube-config=/your-kube-config.conf --cluster-name=test --class-name=your-main-class --jar-path=/your-job-jar.jar

Show more options with the command:

    java -jar com.nextbreakpoint.flink-k8s-toolbox-1.1.1-beta.jar upload jar --help

### How to run the Operator for testing

The Flink operator can be executed as Docker image or as JAR file.   

Run the operator with a given namespace and Kubernetes config using the JAR file:

    java -jar com.nextbreakpoint.flink-k8s-toolbox:1.1.1-beta.jar operator run --namespace=test --kube-config=/path/admin.conf

Or run the operator with a given namespace and Kubernetes config using the Docker image:

    docker run --rm -it -v /path/admin.conf:/admin.conf flink-k8s-toolbox:1.1.1-beta operator run --namespace=test --kube-config=/admin.conf
