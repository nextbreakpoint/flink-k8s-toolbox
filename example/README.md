# Example of Flink deployment on Minikube   

Follow these tutorial to set up a Minikube environment and learn how to use the Flink Kubernetes Toolbox.

## Preparation   

Install Minikube 1.11.0 or later with Kubernetes 1.17. Install kubectl and Helm 3.

Start Minikube with at least 8Gb of memory:

    minikube start --cpus=2 --memory=8gb --kubernetes-version v1.17.0

## Install Minio 

Minio will be used as persistent storage for savepoints.

Install Minio with Helm:

    helm install minio example/minio --namespace flink-jobs --set minio.accessKey=minioaccesskey,minio.secretKey=miniosecretkey

Create an empty bucket:

    kubectl -n flink-jobs run minio-client --image=minio/mc:latest --restart=Never --command=true -- sh -c "mc config host add minio http://minio-headless:9000 minioaccesskey miniosecretkey && mc mb --region=eu-west-1 minio/flink"

Verify that the bucket has been created:

    kubectl -n flink-jobs logs minio-client

    Added `minio` successfully.
    Bucket created successfully `minio/flink`.

## Install Flink Kubernetes Toolbox    

Create flink-operator namespace:

    kubectl create namespace flink-operator

Create flink-jobs namespace:

    kubectl create namespace flink-jobs

Install toolbox resources:

    helm install flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd
    helm install flink-k8s-toolbox-roles helm/flink-k8s-toolbox-roles --namespace flink-operator --set observedNamespace=flink-jobs
    helm install flink-k8s-toolbox-operator helm/flink-k8s-toolbox-operator --namespace flink-operator --set observedNamespace=flink-jobs

Start the operator:

    kubectl scale deployment -n flink-operator flink-operator --replicas=1

Tail the logs of the operator:

    kubectl -n flink-operator logs -f --tail=-1 -l app=flink-operator

## Prepare Flink image    

Configure Docker environment:

    eval $(minikube docker-env)

Build custom Flink image:

    docker build -t example/flink:1.9.2 --build-arg flink_version=1.9.2 --build-arg scala_version=2.11 example/flink

## Prepare Flink jobs    

Configure Docker environment:

    eval $(minikube docker-env)

Build Flink jobs image:

    docker build -t example/jobs:latest example/jobs 

## Use deployment resource to deploy cluster and jobs    

Create configuration:

    kubectl -n flink-jobs create -f example/flink-config.yaml

Create secrets:

    kubectl -n flink-jobs create -f example/flink-secrets.yaml

Create sample data:

    kubectl -n flink-jobs create -f example/sample-data.yaml

Create deployment resource:

    kubectl -n flink-jobs create -f example/deployment.yaml

Get deployment resource:

    kubectl -n flink-jobs get fd cluster-1 -o yaml

Watch cluster resources:

    kubectl -n flink-jobs get fc --watch

Watch job resources:

    kubectl -n flink-jobs get fj --watch

Watch pod resources:

    kubectl -n flink-jobs get pods --watch

Tail the logs of the supervisor:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=supervisor

Tail the logs of the JobManager:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=jobmanager -c jobmanager

Tail the logs of the TaskManager:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=taskmanager -c taskmanager

Expose the JobManager web console:

    kubectl -n flink-jobs port-forward service/jobmanager-cluster-1 8081

then open a browser at http://localhost:8081

## Patch a deployment resource to update cluster and jobs

Example of patch operation to change pullPolicy:

    kubectl -n flink-jobs patch fd cluster-1 --type=json -p '[{"op":"replace","path":"/spec/runtime/pullPolicy","value":"Always"}]'

Example of patch operation to change serviceMode:

    kubectl -n flink-jobs patch fd cluster-1 --type=json -p '[{"op":"replace","path":"/spec/jobManager/serviceMode","value":"ClusterIP"}]'

Example of patch operation to change savepointInterval:

    kubectl -n flink-jobs patch fd cluster-1 --type=json -p '[{"op":"replace","path":"/spec/jobs/0/spec/savepoint/savepointInterval","value":60}]'

## Inspect cluster and jobs

Inspect status of the deployment:

    kubectl -n flink-jobs get fd cluster-1 -o json | jq '.status' 

Inspect status of the cluster:

    kubectl -n flink-jobs get fc cluster-1 -o json | jq '.status' 

Inspect status of a job:

    kubectl -n flink-jobs get fj cluster-1-job-1 -o json | jq '.status' 

## Control cluster and jobs with annotations 

Annotate the cluster to stop it:

    kubectl -n flink-jobs annotate fc cluster-1 --overwrite operator.nextbreakpoint.com/requested-action=STOP 

Annotate the cluster to start it:

    kubectl -n flink-jobs annotate fc cluster-1 --overwrite operator.nextbreakpoint.com/requested-action=START 

Annotate a job to stop it:

    kubectl -n flink-jobs annotate fj cluster-1-job-1 --overwrite operator.nextbreakpoint.com/requested-action=STOP 

Annotate a job to start it:

    kubectl -n flink-jobs annotate fj cluster-1-job-1 --overwrite operator.nextbreakpoint.com/requested-action=START 

##  Control cluster and jobs with curl

Expose operator control interface:

    kubectl -n flink-operator port-forward service/flink-operator 4444

Get status of the deployment:

    curl http://localhost:4444/api/v1/deployments/cluster-1/status | jq -r '.output' | jq

Get status of the cluster:

    curl http://localhost:4444/api/v1/clusters/cluster-1/status | jq -r '.output' | jq

Get status of a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/status | jq -r '.output' | jq

Stop the cluster:

    curl http://localhost:4444/api/v1/clusters/cluster-1/stop -XPUT -d'{"withoutSavepoint":false}'

Start the cluster:

    curl http://localhost:4444/api/v1/clusters/cluster-1/start -XPUT -d'{"withoutSavepoint":false}'

Stop a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/stop -XPUT -d'{"withoutSavepoint":false}'

Start a job:

    curl http://localhost:4444/api/v1/clusters/cluster-1/jobs/job-1/start -XPUT -d'{"withoutSavepoint":false}'

## Control cluster and jobs with flinkctl 

Expose the operator using an external address:

    kubectl -n flink-operator expose service flink-operator --name=flink-operator-external --port=4444 --target-port=4444 --external-ip=$(minikube ip)

Configure Docker environment:

    eval $(minikube docker-env)

Get status of the deployment:

     docker run --rm -it nextbreakpoint/flinkctl:1.4.0-beta deployment status --host=$(minikube ip) --deployment-name=cluster-1 | jq -r '.output' | jq

Get status of the cluster:

     docker run --rm -it nextbreakpoint/flinkctl:1.4.0-beta cluster status --host=$(minikube ip) --cluster-name=cluster-1 | jq -r '.output' | jq

Get status of a job:

     docker run --rm -it nextbreakpoint/flinkctl:1.4.0-beta job status --host=$(minikube ip) --cluster-name=cluster-1 --job-name=job-1 | jq -r '.output' | jq

Stop the cluster:

     docker run --rm -it nextbreakpoint/flinkctl:1.4.0-beta cluster stop --host=$(minikube ip) --cluster-name=cluster-1

Start the cluster:

     docker run --rm -it nextbreakpoint/flinkctl:1.4.0-beta cluster start --host=$(minikube ip) --cluster-name=cluster-1

Stop a job:

     docker run --rm -it nextbreakpoint/flinkctl:1.4.0-beta job stop --host=$(minikube ip) --cluster-name=cluster-1 --job-name=job-1

Start a job:

     docker run --rm -it nextbreakpoint/flinkctl:1.4.0-beta job start --host=$(minikube ip) --cluster-name=cluster-1 --job-name=job-1

## Remove all resources 

Delete the deployment to remove all resources (this might take a while):

    kubectl -n flink-jobs delete fd cluster-1

Wait until the operator has removed everything, then remove the toolbox.

Start the operator:

    kubectl scale deployment -n flink-operator flink-operator --replicas=0

Uninstall toolbox resources:

    helm uninstall flink-k8s-toolbox-operator --namespace flink-operator
    helm uninstall flink-k8s-toolbox-roles --namespace flink-operator
    helm uninstall flink-k8s-toolbox-crd 

