# Flink Kubernetes Toolbox - Manual

This document describes how to install and use the Flink Kubernetes Toolbox.

## Generate SSL certificates

The toolbox provides client and server components. The client component communicates to the server component over HTTP.
To ensure that the communication is secure, flinkctl uses HTTPS and SSL certificates for authentication. 
The HTTPS protocol is optional, and flinkctl can use just HTTP, but in that case it is recommended to restrict the access to port 4444. 

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

Create a secret which contains the keystore and truststore files (optional):

    kubectl -n flink-operator create secret generic flink-operator-ssl \
        --from-file=keystore.jks=secrets/keystore-operator-api.jks \
        --from-file=truststore.jks=secrets/truststore-operator-api.jks \
        --from-literal=keystore-secret=keystore-password \
        --from-literal=truststore-secret=truststore-password

The name of the secret can be any name you like.

Install the CRDs (Custom Resource Definitions) with Helm command:

    helm install flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd

Install the default roles with Helm command:

    helm install flink-k8s-toolbox-roles helm/flink-k8s-toolbox-roles --namespace flink-operator --set observedNamespace=flink-jobs

Install the operator and enable SSL with Helm command:

    helm install flink-k8s-toolbox-operator helm/flink-k8s-toolbox-operator --namespace flink-operator --set observedNamespace=flink-jobs --set secretName=flink-operator-ssl

Remove "--set secretName=flink-operator-ssl" if you don't want to enable SSL.

Scale the operator (only one replica is currently supported) with command:

    kubectl -n flink-operator scale deployment flink-operator --replicas=1

Alternatively, you can pass the argument --set replicas=1 when installing the operator with Helm.

## Uninstall toolbox

Delete all FlinkDeployment resources:

    kubectl -n flink-jobs delete fd --all

and wait until the resources are deleted.

Delete all FlinkCluster resource:

    kubectl -n flink-jobs delete fc --all

and wait until the resources are deleted.

Delete all FlinkJob resource:

    kubectl -n flink-jobs delete fj --all

and wait until the resources are deleted.

Stop the operator with command:

    kubectl -n flink-operator scale deployment flink-operator --replicas=0

Remove the operator with command:    

    helm uninstall flink-k8s-toolbox-operator --namespace flink-operator

Remove the default roles with command:    

    helm uninstall flink-k8s-toolbox-roles --namespace flink-operator

Remove the CRDs with command:    

    helm uninstall flink-k8s-toolbox-crd

Remove secret with command:    

    kubectl -n flink-operator delete secret flink-operator-ssl

Remove operator namespace with command:    

    kubectl delete namespace flink-operator

Remove Flink jobs namespace with command:    

    kubectl delete namespace flink-jobs

Please note that Kubernetes is not able to remove all resources until there are finalizers pending.
The operator is responsible for removing the finalizers but, in case of misconfiguration, it might
not be able to properly remove the finalizers. If you are in that situation, you can always manually
remove the finalizers to allow Kubernetes to delete all resources. However, remember that without
finalizers the operator won't be able to terminate all resources, which ultimately have to be removed manually.

## Upgrade toolbox

PLEASE NOTE THAT THE OPERATOR IS STILL IN BETA VERSION AND IT DOESN'T HAVE A STABLE API YET, THEREFORE EACH RELEASE MIGHT INTRODUCE BREAKING CHANGES.

Before upgrading to a new release, you must cancel all jobs creating a savepoint into a durable storage location (for instance AWS S3).

Create a copy of your FlinkDeployment resources:

    kubectl -n flink-operator get fd -o yaml > deployments-backup.yaml

Create a copy of your FlinkCluster resources:

    kubectl -n flink-operator get fc -o yaml > clusters-backup.yaml

Create a copy of your FlinkJob resources:

    kubectl -n flink-operator get fj -o yaml > jobs-backup.yaml

Upgrade the default roles using Helm:

    helm upgrade flink-k8s-toolbox-roles --install helm/flink-k8s-toolbox-roles --namespace flink-operator --set observedNamespace=flink-jobs

Upgrade the CRDs using Helm:

    helm upgrade flink-k8s-toolbox-crd --install helm/flink-k8s-toolbox-crd

After installing the new CRDs, you can recreate all the custom resources. However, the old resources might not be compatible with the new CRDs.
If that is the case, then you have to fix each resource's specification editing the yaml file and then recreate the resource. You might be
interested of restoring the latest savepoint in the jobs resource. The savepoint path can be copied from the backup into the new resource.

Finally, upgrade and restart the operator using Helm:

    helm upgrade flink-k8s-toolbox-operator --install helm/flink-k8s-toolbox-operator --namespace flink-operator --set observedNamespace=flink-jobs --set secretName=flink-operator-ssl --set replicas=1

## Custom resources and schemas

FlinkDeployment, FlinkCluster and FlinkJob resources can be created, deleted, and inspected using the kubectl command as any other Kubernetes resource.

The CRDs and their schemas are defined in the Helm templates:

    https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/helm/flink-k8s-toolbox-crd/templates/flinkdeployment.yaml
    https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/helm/flink-k8s-toolbox-crd/templates/flinkcluster.yaml
    https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/helm/flink-k8s-toolbox-crd/templates/flinkjob.yaml

The schema of the resource contains the documentation for each field, and it can be consulted as reference when creating new resource:

    kubectl get crd -o json flinkdeployments.nextbreakpoint.com | jq '.spec.versions[0].schema.openAPIV3Schema.properties' > schemas/flinkdeployment-schema.json
    kubectl get crd -o json flinkclusters.nextbreakpoint.com | jq '.spec.versions[0].schema.openAPIV3Schema.properties' > schemas/flinkcluster-schema.json
    kubectl get crd -o json flinkjobs.nextbreakpoint.com | jq '.spec.versions[0].schema.openAPIV3Schema.properties' > schemas/flinkjob-schema.json

Do not delete the CRDs unless you are happy to delete the resources depending on them. Upgrade the CRDs instead of deleting and recreating them:

    helm upgrade flink-k8s-toolbox-crd --install helm/flink-k8s-toolbox-crd  

## Create your first deployment

Make sure that CRDs and default roles have been installed in Kubernetes (see above).

Pull the flinkctl's Docker image with command:

    docker pull nextbreakpoint/flinkctl:1.4.1-beta

Create a new Docker image using flinkctl as base image. This image will be used to run the jobs, 
therefore the image must contain the code of a Flink application package into a single JAR file:

    FROM nextbreakpoint/flinkctl:1.4.1-beta
    COPY flink-jobs.jar /flink-jobs.jar

If you don't have the JAR of a Flink application yet, you can try this:

    FROM busybox AS build
    RUN wget -O flink-jobs.jar https://github.com/nextbreakpoint/flink-workshop/releases/download/v1.2.3/com.nextbreakpoint.flinkworkshop-1.2.3.jar
    FROM nextbreakpoint/flinkctl:1.4.1-beta
    COPY --from=build /flink-jobs.jar /flink-jobs.jar

Build the Docker image with command:

    docker build -t jobs:latest .

Pull the Flink's Docker image with command:

    docker pull flink:1.9.2

Please note that you can use any image of Flink which implements the same entrypoint for running JobManager and TaskManager.

Create a flink-config.yaml file (this is just an example):

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

Deploy the config map:

    kubectl -n flink-jobs apply -f flink-config.yaml 

Create a flink-secrets.yaml file (this is just an example):

    apiVersion: v1
    kind: Secret
    metadata:
        name: flink-secrets
    type: Opaque
    data:
        AWS_ACCESS_KEY_ID: "your-base64-encoded-access-key"
        AWS_SECRET_KEY: "your-base64-encoded-secret-key"

Deploy the secret:

    kubectl -n flink-jobs apply -f flink-secrets.yaml 

Create a file deployment.yaml:

    apiVersion: "nextbreakpoint.com/v1"
    kind: FlinkDeployment
    metadata:
      name: cluster-1
    spec:
      cluster:
        supervisor:
          pullPolicy: IfNotPresent
          image: nextbreakpoint/flinkctl:1.4.1-beta
          serviceAccount: flink-supervisor
          taskTimeout: 180
          rescaleDelay: 10
          resources:
            limits:
              cpu: '0.05'
              memory: 200Mi
            requests:
              cpu: '0.05'
              memory: 200Mi
        runtime:
          pullPolicy: IfNotPresent
          image: flink:1.9.2
        jobManager:
          serviceMode: NodePort
          annotations:
            managedBy: "flinkctl"
          environment:
          - name: FLINK_JM_HEAP
            value: "256"
          environmentFrom:
          - secretRef:
              name: flink-secrets
          volumeMounts:
            - name: config-vol
              mountPath: /hadoop/etc/core-site.xml
              subPath: core-site.xml
          volumes:
            - name: config-vol
              configMap:
                name: flink-config
          extraPorts:
            - name: prometheus
              containerPort: 9999
              protocol: TCP
          resources:
            limits:
              cpu: '1'
              memory: 300Mi
            requests:
              cpu: '0.2'
              memory: 200Mi
        taskManager:
          taskSlots: 2
          annotations:
            managedBy: "flinkctl"
          environment:
          - name: FLINK_TM_HEAP
            value: "1024"
          environmentFrom:
          - secretRef:
              name: flink-secrets
          volumeMounts:
            - name: config-vol
              mountPath: /hadoop/etc/core-site.xml
              subPath: core-site.xml
          volumes:
            - name: config-vol
              configMap:
                name: flink-config
          extraPorts:
            - name: prometheus
              containerPort: 9999
              protocol: TCP
          resources:
            limits:
              cpu: '1'
              memory: 1100Mi
            requests:
              cpu: '0.2'
              memory: 600Mi
      jobs:
        - name: job-1
          spec:
            jobParallelism: 2
            savepoint:
              savepointMode: Automatic
              savepointInterval: 3600
              savepointTargetPath: s3a://flink/cluster-1/job-1/savepoints
            restart:
              restartPolicy: OnlyIfFailed
              restartDelay: 60
              restartTimeout: 120
            bootstrap:
              serviceAccount: flink-bootstrap
              pullPolicy: IfNotPresent
              image: jobs:latest
              jarPath: /flink-jobs.jar
              className: com.nextbreakpoint.flink.jobs.stream.TestJob
              arguments:
                - --CONSOLE_OUTPUT
                - "true"
              resources:
                limits:
                  cpu: '0.05'
                  memory: 200Mi
                requests:
                  cpu: '0.05'
                  memory: 200Mi
        - name: job-2
          spec:
            jobParallelism: 1
            savepoint:
              savepointMode: Automatic
              savepointInterval: 0
              savepointTargetPath: s3a://flink/cluster-1/job-2/savepoints
            restart:
              restartPolicy: Always
              restartDelay: 60
              restartTimeout: 120
            bootstrap:
              serviceAccount: flink-bootstrap
              pullPolicy: IfNotPresent
              image: jobs:latest
              jarPath: /flink-jobs.jar
              className: com.nextbreakpoint.flink.jobs.stream.TestJob
              arguments:
                - --CONSOLE_OUTPUT
                - "true"
              resources:
                limits:
                  cpu: '0.05'
                  memory: 200Mi
                requests:
                  cpu: '0.05'
                  memory: 200Mi
        - name: job-3
          spec:
            jobParallelism: 1
            savepoint:
              savepointMode: Manual
              savepointInterval: 0
              savepointTargetPath: s3a://flink/cluster-1/job-3/savepoints
            restart:
              restartPolicy: Never
              restartDelay: 60
              restartTimeout: 120
            bootstrap:
              serviceAccount: flink-bootstrap
              pullPolicy: IfNotPresent
              image: jobs:latest
              jarPath: /flink-jobs.jar
              className: com.nextbreakpoint.flink.jobs.stream.TestJob
              arguments:
                - --CONSOLE_OUTPUT
                - "true"
              resources:
                limits:
                  cpu: '0.05'
                  memory: 200Mi
                requests:
                  cpu: '0.05'
                  memory: 200Mi

Create the resource with command:

    kubectl -n flink-jobs apply -f deployment.yaml

At this point, the operator should create a bunch of derived resources, including one FlinkCluster resource, three FlinkJob resources,  
one Service resource, and several Pods resources. The name of the resources contains the name of the cluster, which is the same as the name of the deployment.   
Wait few minutes until the operator creates the supervisor of the cluster, deploys JobManager and TaskManagers, and starts the Flink jobs.

You can follow what the operator is doing:

    kubectl -n flink-operator logs -f --tail=-1 -l app=flink-operator

    2020-12-16 09:57:40 INFO CacheAdapter - Starting watch loop for resources: FlinkDeployments
    2020-12-16 09:57:40 INFO CacheAdapter - Starting watch loop for resources: FlinkClusters
    2020-12-16 09:57:40 INFO CacheAdapter - Starting watch loop for resources: FlinkJobs
    2020-12-16 09:57:40 INFO CacheAdapter - Starting watch loop for resources: Deployments
    2020-12-16 09:57:40 INFO CacheAdapter - Starting watch loop for resources: Pods
    2020-12-16 09:57:40 INFO OperatorVerticle - HTTPS with client authentication is enabled
    [vert.x-eventloop-thread-0] INFO io.vertx.ext.web.handler.impl.LoggerHandlerImpl - 172.17.0.1 - - [Wed, 16 Dec 2020 09:57:57 GMT] "GET /version HTTP/1.1" 200 0 "-" "kube-probe/1.18"

You can follow what the supervisor is doing:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=supervisor

    2020-12-16 09:57:50 INFO CacheAdapter - Starting watch loop for resources: Services
    2020-12-16 09:57:50 INFO CacheAdapter - Starting watch loop for resources: FlinkJobs
    2020-12-16 09:57:50 INFO CacheAdapter - Starting watch loop for resources: Jobs
    2020-12-16 09:57:50 INFO CacheAdapter - Starting watch loop for resources: Pods
    2020-12-16 09:57:51 WARNING SupervisorVerticle - HTTPS not enabled!
    2020-12-16 09:57:50 INFO CacheAdapter - Starting watch loop for resources: FlinkClusters
    2020-12-16 09:57:55 INFO Supervisor cluster-1 - Resource version: 4499685
    2020-12-16 09:57:55 INFO Supervisor cluster-1 - Supervisor status: Unknown
    2020-12-16 09:57:55 INFO Supervisor cluster-1 - Add finalizer
    2020-12-16 09:57:56 INFO Supervisor cluster-1-job-1 - Resource version: 4499646
    2020-12-16 09:57:56 INFO Supervisor cluster-1-job-1 - Supervisor status: Unknown
    2020-12-16 09:57:56 INFO Supervisor cluster-1-job-1 - Add finalizer

You can watch the FlinkDeployment resource:

    kubectl -n flink-jobs get fd --watch

    NAME        RESOURCE-STATUS   AGE
    cluster-1   Updated           6m20s

You can watch the FlinkCluster resource:

    kubectl -n flink-jobs get fc --watch

    NAME        RESOURCE-STATUS   SUPERVISOR-STATUS   CLUSTER-HEALTH   TASK-MANAGERS   TOTAL-TASK-SLOTS   SERVICE-MODE   AGE
    cluster-1   Updating          Starting                             0               0                  NodePort       2m33s
    cluster-1   Updated           Started             HEALTHY          2               4                  NodePort       5m

You can watch the FlinkJob resources:

    kubectl -n flink-jobs get fj --watch

    NAME              RESOURCE-STATUS   SUPERVISOR-STATUS   CLUSTER-NAME   CLUSTER-HEALTH   JOB-STATUS   JOB-ID                             JOB-PARALLELISM   JOB-RESTART    SAVEPOINT-MODE   SAVEPOINT-PATH   SAVEPOINT-AGE   AGE
    cluster-1-job-1   Updating          Starting            cluster-1                                                                       2                 OnlyIfFailed   Automatic                                         2m48s
    cluster-1-job-2   Updating          Starting            cluster-1                                                                       1                 Always         Automatic                                         2m48s
    cluster-1-job-3   Updating          Starting            cluster-1                                                                       1                 Never          Manual                                            2m48s
    cluster-1-job-1   Updated           Started             cluster-1      HEALTHY          RUNNING      82cf4d4e6c0cd4f89b587c74af33f298   2                 OnlyIfFailed   Automatic                                         7m

You can watch the pods:

    kubectl -n flink-jobs get pod --watch

    NAME                                    READY   STATUS      RESTARTS   AGE
    bootstrap-cluster-1-job-1-msbrv         0/1     Completed   0          3m3s
    jobmanager-cluster-1-dmpk9              1/1     Running     0          10m
    minio-f7ddc4b9d-f99qr                   1/1     Running     16         128d
    supervisor-cluster-1-7cfb9c76f4-l76gt   1/1     Running     0          18m
    taskmanager-cluster-1-d8xzg             1/1     Running     0          3m15s
    taskmanager-cluster-1-ll88q             1/1     Running     0          3m15s

You can inspect the FlinkDeployment resource:

    kubectl -n flink-jobs get fd cluster-1 -o json | jq '.status'

    {
        "digest": {
            "cluster": {
                "jobManager": "CTfL1eq/ae2HDbSRWWkTKg==",
                "runtime": "RMgn5qo3Q+Qy706ghyxeMw==",
                "supervisor": "WtUIhzGMVrfpgNNH8oe/uw==",
                "taskManager": "gQcoI/m7uD7hnM6bjVxtmA=="
            },
            "jobs": [
                {
                    "job": {
                        "bootstrap": "qOety+rnp6ETxBsejANgow==",
                        "restart": "PQvmPQxmz6LjAa8HW+SQPQ==",
                        "savepoint": "oxNouuvcYqYkBWtMOnKjdQ=="
                    },
                    "name": "job-1"
                },
                {
                    "job": {
                        "bootstrap": "s/EoBI5x9h7xtvlvOCKMGw==",
                        "restart": "pxeYsVQV2fsVkJfRUQgehw==",
                        "savepoint": "oj/YaUKHUIZHCjgRxVN2wA=="
                    },
                    "name": "job-2"
                },
                {
                    "job": {
                        "bootstrap": "40m+cgqz3RTs9Pps7vy25Q==",
                        "restart": "+pseQWz5urZaK5Feo97SsA==",
                        "savepoint": "fCBVDBHjaBLHu1lMugtOuA=="
                    },
                    "name": "job-3"
                }
            ]
        },
        "resourceStatus": "Updated",
        "timestamp": "2020-12-16T09:57:38.132Z"
    }

You can inspect the FlinkCluster resource:

    kubectl -n flink-jobs get fc cluster-1 -o json | jq '.status'

    {
        "clusterHealth": "HEALTHY",
        "digest": {
            "jobManager": "CTfL1eq/ae2HDbSRWWkTKg==",
            "runtime": "RMgn5qo3Q+Qy706ghyxeMw==",
            "supervisor": "WtUIhzGMVrfpgNNH8oe/uw==",
            "taskManager": "gQcoI/m7uD7hnM6bjVxtmA=="
        },
        "labelSelector": "name=cluster-1,owner=flink-operator,component=flink,role=taskmanager",
        "rescaleTimestamp": "2020-12-16T10:12:55.416Z",
        "resourceStatus": "Updated",
        "serviceMode": "NodePort",
        "supervisorStatus": "Started",
        "taskManagerReplicas": 2,
        "taskManagers": 2,
        "taskSlots": 2,
        "timestamp": "2020-12-16T10:13:07.026Z",
        "totalTaskSlots": 4
    }

You can inspect the FlinkJob resources:

    kubectl -n flink-jobs get fj cluster-1-job-1 -o json | jq '.status'

    {
        "clusterHealth": "HEALTHY",
        "clusterName": "cluster-1",
        "digest": {
            "bootstrap": "qOety+rnp6ETxBsejANgow==",
            "restart": "PQvmPQxmz6LjAa8HW+SQPQ==",
            "savepoint": "oxNouuvcYqYkBWtMOnKjdQ=="
        },
        "jobId": "82cf4d4e6c0cd4f89b587c74af33f298",
        "jobParallelism": 2,
        "jobStatus": "RUNNING",
        "labelSelector": "name=cluster-1-job-1,owner=flink-operator,component=flink,role=taskmanager",
        "resourceStatus": "Updated",
        "restartPolicy": "OnlyIfFailed",
        "savepointJobId": "",
        "savepointMode": "Automatic",
        "savepointPath": "",
        "savepointRequestTimestamp": "2020-12-16T09:58:02.343Z",
        "savepointTriggerId": "",
        "supervisorStatus": "Started",
        "timestamp": "2020-12-16T10:13:30.628Z"
    }

You can also expose the JobManager web console:

    kubectl -n flink-jobs port-forward service/jobmanager-cluster-1 8081

then open a browser at http://localhost:8081

## Control cluster and jobs with annotations

Annotate the cluster to stop it:

    kubectl -n flink-jobs annotate fc cluster-1 --overwrite operator.nextbreakpoint.com/requested-action=STOP 

Annotate the cluster to start it:

    kubectl -n flink-jobs annotate fc cluster-1 --overwrite operator.nextbreakpoint.com/requested-action=START 

Annotate a job to stop it:

    kubectl -n flink-jobs annotate fj cluster-1-job-1 --overwrite operator.nextbreakpoint.com/requested-action=STOP 

Annotate a job to start it:

    kubectl -n flink-jobs annotate fj cluster-1-job-1 --overwrite operator.nextbreakpoint.com/requested-action=START 

Annotate a job to trigger a savepoint:

    kubectl -n flink-jobs annotate fj cluster-1-job-1 --overwrite operator.nextbreakpoint.com/requested-action=TRIGGER_SAVEPOINT 

Annotate a job to forget a savepoint:

    kubectl -n flink-jobs annotate fj cluster-1-job-1 --overwrite operator.nextbreakpoint.com/requested-action=FORGET_SAVEPOINT 

## Control cluster and jobs using HTTP  

The operator exposes a control interface as REST API (port 4444 by default).

The control interface can be used to fetch status, details and metrics, and to submit commands which can be understood by the operator and the supervisor.

The following endpoints support GET requests:

    /deployments
    /deployments/<deploymentname>/status
    /clusters
    /cluster/<clustername>/status
    /cluster/<clustername>/jobs
    /cluster/<clustername>/jobs/<jobname>/status
    /cluster/<clustername>/jobs/<jobname>/details
    /cluster/<clustername>/jobs/<jobname>/metrics
    /cluster/<clustername>/jobmanager/metrics
    /cluster/<clustername>/taskmanagers
    /cluster/<clustername>/taskmanagers/<taskmanagerid>/details
    /cluster/<clustername>/taskmanagers/<taskmanagerid>/metrics

The following endpoints support POST requests:

    /deployments/<deploymentname>
    /clusters/<clustername>
    /clusters/<clustername>/jobs/<jobname>

The following endpoints support DELETE requests:

    /deployments/<deploymentname>
    /clusters/<clustername>
    /clusters/<clustername>/jobs/<jobname>
    /clusters/<clustername>/jobs/<jobname>/savepoint

The following endpoints support PUT requests:

    /deployments/<deploymentname>
    /clusters/<clustername>
    /clusters/<clustername>/jobs/<jobname>
    /clusters/<clustername>/start
    /clusters/<clustername>/stop
    /clusters/<clustername>/scale
    /clusters/<clustername>/jobs/<jobname>/start
    /clusters/<clustername>/jobs/<jobname>/stop
    /clusters/<clustername>/jobs/<jobname>/scale
    /clusters/<clustername>/jobs/<jobname>/savepoint

Expose the control interface to your host:

    kubectl -n flink-operator port-forward service/flink-operator 4444

Get list of deployments:

    curl http://localhost:4444/api/v1/deployments

Get list of clusters:

    curl http://localhost:4444/api/v1/clusters

Get list of jobs:

    curl http://localhost:4444/api/v1/clusters/custer-1/jobs

Get status of a deployment:

    curl http://localhost:4444/api/v1/deployments/cluster-1/status

Get status of a cluster:

    curl http://localhost:4444/api/v1/clusters/cluster-1/status

Get status of a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/status

Get details of a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/details

Get metrics of a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/metrics

Get metrics of the JobManager:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobmanager/metrics

Get list of TaskManagers:

    curl http://localhost:4444/api/v1/clusters/cluster-1/taskmanagers

Get metrics of a TaskManager:

    curl http://localhost:4444/api/v1/clusters/cluster-1/taskmanagers/67761be7be3c93b44dd037632871c828/metrics

Get details of a TaskManager:

    curl http://localhost:4444/api/v1/clusters/cluster-1/taskmanagers/67761be7be3c93b44dd037632871c828/details

Create a deployment:

    curl http://localhost:4444/api/v1/deployments/cluster-1 -XPOST -d@example/deployment-spec.json

Create a cluster:

    curl http://localhost:4444/api/v1/clusters/cluster-1 -XPOST -d@example/cluster-spec.json

Create a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1 -XPOST -d@example/job-spec-1.json

Delete a deployment:

    curl http://localhost:4444/api/v1/deployments/cluster-1 -XDELETE

Delete a cluster:

    curl http://localhost:4444/api/v1/clusters/cluster-1 -XDELETE

Delete a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1 -XDELETE

Update a deployment:

    curl http://localhost:4444/api/v1/deployments/cluster-1 -XPUT -d@example/deployment-spec.json

Update a cluster:

    curl http://localhost:4444/api/v1/clusters/cluster-1 -XPUT -d@example/cluster-spec.json

Update a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1 -XPUT -d@example/job-spec-1.json

Start a cluster:

    curl http://localhost:4444/api/v1/clusters/cluster-1/start -XPUT -d'{"withoutSavepoint":false}'

Stop a cluster:

    curl http://localhost:4444/api/v1/clusters/cluster-1/stop -XPUT -d'{"withoutSavepoint":false}'

Scale a cluster:

    curl http://localhost:4444/api/v1/clusters/cluster-1/scale -XPUT -d'{"taskManagers":4}'

Start a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/start -XPUT -d'{"withoutSavepoint":false}'

Stop a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/stop -XPUT -d'{"withoutSavepoint":false}'

Scale a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/scale -XPUT -d'{"parallelism":2}'

Trigger a savepoint:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/savepoint/trigger -XPUT

Forget a savepoint:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/savepoint/forget -XPUT

Please note that you must use SSL certificates when invoking the API if the operator has SSL enabled (see instructions for generating SSL certificates above):

    curl --cacert secrets/ca_cert.pem --cert secrets/operator-cli_cert.pem --key secrets/operator-cli_key.pem https://localhost:4444/api/v1/clusters/<name>/status

## Control cluster and jobs using flinkctl

Print the CLI usage:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta --help

The output should look like:

    Usage: flinkctl [OPTIONS] COMMAND [ARGS]...

    Options:
        -h, --help  Show this message and exit
    
    Commands:
        operator      Access operator subcommands
        supervisor    Access supervisor subcommands
        bootstrap     Access bootstrap subcommands
        deployments   Access deployments subcommands
        deployment    Access deployment subcommands
        clusters      Access clusters subcommands
        cluster       Access cluster subcommands
        savepoint     Access savepoint subcommands
        jobs          Access jobs subcommands
        job           Access job subcommands
        jobmanager    Access JobManager subcommands
        taskmanager   Access TaskManager subcommands
        taskmanagers  Access TaskManagers subcommands

You can see the options of each subcommand:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta cluster create --help

    Usage: flinkctl cluster create [OPTIONS]
    
        Create a cluster
    
    Options:
        --host TEXT               The operator host
        --port INT                The operator port
        --keystore-path TEXT      The keystore path
        --keystore-secret TEXT    The keystore secret
        --truststore-path TEXT    The truststore path
        --truststore-secret TEXT  The truststore secret
        --cluster-name TEXT       The name of the Flink cluster
        --cluster-spec TEXT       The specification of the Flink cluster in JSON format
        -h, --help                Show this message and exit

Expose the operator using an external address:

    kubectl -n flink-operator expose service flink-operator --name=flink-operator-external --port=4444 --target-port=4444 --external-ip=$(minikube ip)

Get the list of deployments:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta deployments list --host=$(minikube ip) | jq -r '.output' | jq

Get the list of clusters:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta clusters list --host=$(minikube ip) | jq -r '.output' | jq

Get the list of jobs:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta jobs list --cluster-name=cluster-1 --host=$(minikube ip) | jq -r '.output' | jq

Get the status of a deployment:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta deployment status --deployment-name=cluster-1 --host=$(minikube ip) | jq -r '.output' | jq

Get the status of a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta cluster status --cluster-name=cluster-1 --host=$(minikube ip) | jq -r '.output' | jq

Get the status of a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta job status --cluster-name=cluster-1 --job-name=job-1 --host=$(minikube ip) | jq -r '.output' | jq

Delete a deployment:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta deployment delete --deployment-name=cluster-1 --host=$(minikube ip)

Delete a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta cluster delete --cluster-name=cluster-1 --host=$(minikube ip)

Delete a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta job delete --cluster-name=cluster-1 --job-name=job-1 --host=$(minikube ip)

Stop a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta cluster stop --cluster-name=cluster-1 --host=$(minikube ip)

Start a cluster: 

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta cluster start --cluster-name=cluster-1 --host=$(minikube ip)

Stop a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta job stop --cluster-name=cluster-1 --job-name=job-1 --host=$(minikube ip)

Start a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta job start --cluster-name=cluster-1 --job-name=job-1 --host=$(minikube ip)

Start a cluster without recovering from the savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta cluster start --cluster-name=cluster-1 --without-savepoint --host=$(minikube ip) 

Stop a cluster without creating a new savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta cluster stop --cluster-name=cluster-1 --without-savepoint --host=$(minikube ip)

Start a job without recovering from the savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta job start --cluster-name=cluster-1 --job-name=job-1 --without-savepoint --host=$(minikube ip) 

Stop a job without creating a new savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta job stop --cluster-name=cluster-1 --job-name=job-1 --without-savepoint --host=$(minikube ip)

Create a new savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta savepoint trigger --cluster-name=cluster-1 --job-name=job-1 --host=$(minikube ip) 

Remove savepoint from job:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta savepoint forget --cluster-name=cluster-1 --job-name=job-1 --host=$(minikube ip) 

Rescale a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta cluster scale --cluster-name=cluster-1 --task-managers=4 --host=$(minikube ip) 

Rescale a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta job scale --cluster-name=cluster-1 --job-name=job-1 --parallelism=2 --host=$(minikube ip) 

Get the details of the job: 

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta job details --cluster-name=cluster-1 --job-name=job-1 --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of the job:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta job metrics --cluster-name=cluster-1 --job-name=job-1 --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of the JobManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta jobmanager metrics --cluster-name=cluster-1 --host=$(minikube ip) | jq -r '.output' | jq

Get a list of TaskManagers:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta taskmanagers list --cluster-name=cluster-1 --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of a TaskManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta taskmanager metrics --cluster-name=cluster-1 --taskmanager-id=67761be7be3c93b44dd037632871c828 --host=$(minikube ip) | jq -r '.output' | jq

Get the details of a TaskManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta taskmanager details --cluster-name=cluster-1 --taskmanager-id=67761be7be3c93b44dd037632871c828 --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of a TaskManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta taskmanager metrics --cluster-name=cluster-1 --taskmanager-id=67761be7be3c93b44dd037632871c828 --host=$(minikube ip) | jq -r '.output' | jq

## Server components

The command flinkctl implements both client and server components. The client commands are the ones used to operate on the resources.
The server commands instead are the ones used to run the processes which together implement the Kubernetes Operator pattern.

### Operator process

The operator process is required to orchestrate resources and processes.

    flinkctl operator run --namespace=flink-jobs --task-timeout=180 --polling-interval=5

### Supervisor process

The supervisor process is required to reconcile the status of the resources.

    flinkctl supervisor run --namespace=flink-jobs --cluster-name=cluster-1 --task-timeout=180 --polling-interval=5

### Bootstrap process

The bootstrap process is required to upload the JAR to the JobManager and run the job. 

    flinkctl bootstrap run --namespace=flink-jobs --cluster-name=cluster-1 --job-name=job-1 --class-name=your-main-class --jar-path=/your-job-jar.jar --parallelism=2 

### Monitoring 

The operator process exposes metrics to Prometheus on port 8080 by default:

    http://localhost:8080/metrics

The supervisor process exposes metrics to Prometheus on port 8080 by default:

    http://localhost:8080/metrics

### Healthcheck

The operator process exposes a version endpoint which can be used to check the process is healthy:

    http://localhost:8080/version

The supervisor process exposes a version endpoint which can be used to check the process is healthy:

    http://localhost:8080/version

### State machines

The supervisor implements two state machines for managing clusters and jobs.

The state machine associate to a cluster is documented in this graph:

![Cluster state machine](/graphs/flink-cluster.png "Cluster state machine")

The state machine associate to a job is documented in this graph:

![Job state machine](/graphs/flink-job.png "Job state machine")

## Developers instructions

Instruction for building and running the flinkctl command. 

### Build with Gradle

Install GraalVM 20.3.0 (graalvm-ce-java11-20.3.0).

Compile and package the code with Gradle command:

    ./gradlew build copyRuntimeDeps

All the JARs required to run the application will be produced into the directory build/libs. 

Please note that Gradle alone will only build the JARs, not the binary file. 

If you are on MacOS or Linux, you can build the flinkctl binary file with the command:

    $GRAALVM_HOME/bin/native-image --verbose -cp $(classpath)

where CLASSPATH must include all the dependencies required to run the application:

    export CLASSPATH=$(find build/libs -name "*.jar" -print | sed "s/.jar/.jar:/g" | tr -d '\n' | sed "s/:$//g")

### Build with Docker

Build a Docker image with command:

    docker build -t nextbreakpoint/flinkctl:1.4.1-beta .

Test the image printing the CLI usage:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.1-beta --help

Tag and push the image to your Docker registry if needed:

    docker tag nextbreakpoint/flinkctl:1.4.1-beta some-registry/flinkctl:1.4.1-beta
    docker login some-registry
    docker push some-registry/flinkctl:1.4.1-beta

### Run the application

The command flinkctl can be executed directly on any Linux machine or indirectly from a Docker container on any system which supports Docker. 
Alternatively, you can run the Java code with the java command. The application can be executed and debugged as any Java application. 

The application can be configured to point to a local or remote Kubernetes cluster, and it works with Minikube and Docker for Desktop too.

Run the operator with a given namespace and Kubernetes config on Linux:

    flinkctl operator run --namespace=test --kube-config=~/.kube/config

Run the operator with a given namespace and Kubernetes config using Docker:

    docker run --rm -it -v ~/.kube/config:/kube/config nextbreakpoint/flinkctl:1.4.1-beta operator run --namespace=test --kube-config=/kube/config

Run the operator with a given namespace and Kubernetes config with java command:

    java -cp $CLASSPATH com.nextbreakpoint.flink.cli.Main operator run --namespace=test --kube-config=~/.kube/config

where CLASSPATH must include all the dependencies required to run the application:

    export CLASSPATH=$(find build/libs -name "*.jar" -print | sed "s/.jar/.jar:/g" | tr -d '\n' | sed "s/:$//g")

### Run automated tests

Run unit tests with command:

    ./gradlew clean test

Run integration tests against Docker for Desktop or Minikube with command:

    export BUILD_IMAGES=true
    ./gradlew clean integrationTest

You can skip the Docker images build step if the images already exist:

    export BUILD_IMAGES=false
    ./gradlew clean integrationTest

## Advanced configuration

Instruction for configuring operator, supervisor, and bootstrap processes.

### Configure service accounts

The service account for the operator is defined in the Helm chart, but it can be modified when installing with Helm.

The service account for the supervisor can be configured in the FlinkDeployment and FlinkCluster resources.
If not specified, the service account flink-supervisor will be used for the supervisor.

The service account of the bootstrap can be configured in the FlinkDeployment and FlinkJob resources.
If not specified, the service account flink-bootstrap will be used for the bootstrap.

The service account for JobManagers and TaskManagers can be configured in the FlinkDeployment and FlinkCluster resources.
If not specified, the default service account will be used for JobManagers and TaskManagers.

In case the default account are not used, then new account must be created with the correct permissions.
The required permissions should match the ones which are defined in the default accounts. 

### Periodic savepoints and automatic savepoints

The operator automatically creates savepoints before stopping a job, and automatically starts a job from the latest savepoint when the job restarts.
The job typically restarts when a change is applied to a specification, or the cluster is rescaled, or the job parallelism is changed, or the job is stopped manually.
This feature is very handy to avoid losing the status of the job when rolling out an update, or in case of temporary failure.

However, for this feature to work properly, the savepoints must be created in a durable storage location such as HDFS or S3.
Only a durable location can be used to recover the job after recreating the JobManager and the TaskManagers.

### Configure task timeout

The operator and the supervisor use timeouts to recover for anomalies.

The duration of the timeout for the operator can be changed when installing the operator with Helm.

The duration of the timeout for the supervisor can be changed in the resource specification.    

### Configure polling interval

The operator and the supervisor poll periodically the status of the resources.

The duration of the polling interval for the operator can be changed when installing the operator with Helm.

The duration of the polling interval for the supervisor can be changed in the resource specification.
