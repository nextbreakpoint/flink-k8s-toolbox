# Flink Kubernetes Toolbox

Flink Kubernetes Toolbox is the Swiss Army knife for deploying and managing Apache Flink on Kubernetes.
The toolbox provides a native command flinkctl which can be executed on Linux machines or Docker containers.
The command implements both client and server components which together represent a complete solution for operating Apache Flink on Kubernetes.
The command is based on the Kubernetes Operator Pattern, it implements a custom controller, and it works with custom resources.         


## Features

This is the list of the main features implemented in the toolbox:
- Automatically manage JobManagers and TaskManagers
- Automatically manage jobs in the cluster
- Automatically recover from temporary failure
- Automatically restart clusters or jobs when modifying resources
- Automatically create a savepoint before stopping a job (optional)
- Automatically recover from latest savepoint when restarting a job
- Support for deployment, cluster and job resources
- Support for batch and stream jobs
- Support for cluster without jobs (bare cluster)
- Support for cluster with one or more jobs
- Support cluster and job rescale via Kubernetes scale interface
- Support autoscaling based on custom metrics (compatible with HPA)
- Support for init containers and side containers for JobManagers and TaskManagers
- Support for mounted volumes (same as volumes in Pod specification)
- Support for environment variables, including variables from ConfigMap or Secret
- Support for resource requirements (for all components)
- Support for user defined entrypoint
- Support for user defined annotations
- Support for user defined container ports
- Support for pull secrets and private registries
- Support for public Flink images or custom images
- Use separate supervisor for each cluster
- Use separate Docker image for launching a job (single JAR file)
- Configurable service accounts
- Configurable periodic savepoints
- Configurable savepoints location
- CLI and REST interface to support operations
- Metrics compatible with Prometheus


## Overview

At the core of the toolbox there is the operator and its buddy the supervisor.
In a nutshell, the operator and the supervisor are constantly watching some resources,
and they apply modifications to other resources in order to converge to the desired state.

The toolbox supports the following custom resources:
- FlinkDeployment: it represents a cluster deployment, and it provides the configuration of the cluster with an optional list of jobs.
- FlinkCluster: it represents a deployed cluster, and it stores the configuration and the status of the cluster.
- FlinkJob: it represents a deployed job, and it stores the configuration and the status of the job.

The operator can be installed in a namespace which is different from the one where the supervisor and Flink are executed.
The operator detects changes in the custom resources created in the given namespace, and eventually it creates a supervisor
for managing a cluster and its jobs. The operator creates a separate supervisor process for each cluster.
The cluster and the jobs are also called primary resources.

The supervisor detects changes in the primary resources, and it eventually creates,
updates or deletes one or more secondary resources, such as Pods, Services and BatchJobs.  
The supervisor is responsible to reconcile the status of the cluster and its jobs.
The supervisor depends on the Flink's Monitoring REST API, which is used to observe and control the status of the Flink cluster.
The supervisor persists its status in the primary resources, and the status can be inspected with flinkctl or kubectl.
The supervisor can perform several tasks automatically, such as creating savepoints when a job is restarted
or restarting the cluster when the specification changed, which make easier to operate Flink on Kubernetes.  

The deployment resource is convenient for defining a cluster, with multiple jobs, in a single resource,
however, clusters and jobs can be created independently. Jobs can be added or removed to an existing
cluster either updating a deployment resource or directly creating or deleting new job resources.

A job can be created independently of a cluster, but it can only be executed when there is a cluster for executing the job.
A job can be associated to a cluster based on the name of the resources. The rule is that the name of the FlinkJob resource must start
with the name of the corresponding FlinkCluster resource, followed by a separator plus the job name (for instance clustername-jobname).

The dependencies between resources are represented in the following graph:

![Resource dependencies](/graphs/flink-operator.png "Resource dependencies")


## Get started

Download the Docker image with flinkctl command from Docker Hub:

    docker pull nextbreakpoint/flinkctl:1.4.4-beta

Execute flinkctl as Docker container:

    docker run --rm -it nextbreakpoint/flinkctl:1.4.4-beta --help

Check out the quickstart example to get started with the toolbox:

https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/example/README.md

Continue reading the manual for detailed instructions about how to install and how to use the toolbox:

https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/MANUAL.md

Look at the section "Create your first deployment".


## Contribute

Visit the project on GitHub:

https://github.com/nextbreakpoint/flink-k8s-toolbox

Report an issue or request a feature:

https://github.com/nextbreakpoint/flink-k8s-toolbox/issues


## License

The toolbox is distributed under the terms of BSD 3-Clause License.

    Copyright (c) 2019-2021, Andrea Medeghini
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
