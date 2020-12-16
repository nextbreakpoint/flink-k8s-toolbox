# Flink Kubernetes Toolbox

Flink Kubernetes Toolbox is a CLI tool for deploying and managing Apache Flink on Kubernetes.
The toolbox provides a native command flinkctl which can be executed on any Linux or MacOS machine and from a Docker container.
The command implements both client and server components which together represent a complete solution for operating Apache Flink on Kubernetes.
The command is based on the Kubernetes Operator Pattern, it implements a custom controller, and it works with custom resources.         


## Features

This is the list of the main features implemented in the toolbox:
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


## Overview

At the core of the toolbox there is a Kubernetes operator.

In a nutshell, the operator detects changes in custom resources and it applies
modifications to other resources in order to converge to the desired status.
The operator makes use of the Flink Monitoring REST API to control jobs
and observe the status of the cluster.

The operator support the following custom resources:
- FlinkDeployment, it represents the configuration of a cluster with an optional list of jobs.
- FlinkCluster, it represents a cluster, it contains the configuration and the status of the cluster.
- FlinkJob, it represents a job, it contains the configuration and the status of the job.

The deployment resource is convenient for defining a cluster with multiple jobs in a single resource,
however, clusters and jobs can be created independently. A job can only be executed when there is a
cluster for executing the job. A job is associated to a cluster using the name of the cluster.
The requirement is that the name of the FlinkJob must start with the name of the corresponding
FlinkCluster followed by the job name, like clustername-jobname.

The operator can be installed in a separate namespace from the one where Flink is executed.
The operator detects changes in the custom resources, or primary resources, created in the given namespace,
and eventually it creates, updates or deletes one or more secondary resources, such as Pods, Services and Jobs.  

The operator persists its status in the custom resources, and the
status can be inspected with kubectl or directly with flinkctl.

The operator creates and manages a separate supervisor process for each cluster.
The supervisor is responsible to reconcile the status of the cluster and its jobs.
Jobs can be added or removed to an existing cluster either updating a deployment
resource or directly creating or deleting new job resources.

The operator can perform several tasks automatically, such as creating savepoints when a job is restarted
or restarting the cluster when the specification changed, which make easier to operate Flink on Kubernetes.  

The dependencies between primary and secondary resources are represented in this graph:

![Resource dependencies](/graphs/flink-operator.png "Resource dependencies")

Continue reading the manual for detailed instructions about how to install and use the toolbox:

https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/MANUAL.md

Check out the quickstart example if you want to try the toolbox:

https://github.com/nextbreakpoint/flink-k8s-toolbox/blob/master/example/README.md


## License

The toolbox is distributed under the terms of BSD 3-Clause License.

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
