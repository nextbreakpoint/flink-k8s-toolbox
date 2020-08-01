package com.nextbreakpoint.flinkoperator.controller.resources

import io.kubernetes.client.models.V1Pod
import io.kubernetes.client.models.V1Service

data class ClusterResources(
    val service: V1Service?,
    val jobmanagerPod: V1Pod?,
    val taskmanagerPod: V1Pod?
) {
    fun withService(jobmanagerService: V1Service?) =
        ClusterResources(
            service = jobmanagerService,
            jobmanagerPod = this.jobmanagerPod,
            taskmanagerPod = this.taskmanagerPod
        )

    fun withJobManagerPod(jobmanagerPod: V1Pod?) =
        ClusterResources(
            service = this.service,
            jobmanagerPod = jobmanagerPod,
            taskmanagerPod = this.taskmanagerPod
        )

    fun withTaskManagerPod(taskmanagerPod: V1Pod?) =
        ClusterResources(
            service = this.service,
            jobmanagerPod = this.jobmanagerPod,
            taskmanagerPod = taskmanagerPod
        )
}

