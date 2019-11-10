package com.nextbreakpoint.flinkoperator.controller.resources

import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

data class ClusterResources(
    val bootstrapJob: V1Job?,
    val jobmanagerService: V1Service?,
    val jobmanagerStatefulSet: V1StatefulSet?,
    val taskmanagerStatefulSet: V1StatefulSet?
) {
    fun withJobManagerService(jobmanagerService: V1Service?) =
        ClusterResources(
            bootstrapJob = this.bootstrapJob,
            jobmanagerService = jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet
        )

    fun withBootstrapJob(bootstrapJob: V1Job?) =
        ClusterResources(
            bootstrapJob = bootstrapJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet
        )

    fun withJobManagerStatefulSet(jobmanagerStatefulSet: V1StatefulSet?) =
        ClusterResources(
            bootstrapJob = this.bootstrapJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet
        )

    fun withTaskManagerStatefulSet(taskmanagerStatefulSet: V1StatefulSet?) =
        ClusterResources(
            bootstrapJob = this.bootstrapJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet
        )
}

