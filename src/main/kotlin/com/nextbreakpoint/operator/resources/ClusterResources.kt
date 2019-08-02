package com.nextbreakpoint.operator.resources

import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

data class ClusterResources(
    val jarUploadJob: V1Job?,
    val jobmanagerService: V1Service?,
    val jobmanagerStatefulSet: V1StatefulSet?,
    val taskmanagerStatefulSet: V1StatefulSet?
) {
    fun withJobManagerService(jobmanagerService: V1Service?) =
        ClusterResources(
            jarUploadJob = this.jarUploadJob,
            jobmanagerService = jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet
        )

    fun withJarUploadJob(jarUploadJob: V1Job?) =
        ClusterResources(
            jarUploadJob = jarUploadJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet
        )

    fun withJobManagerStatefulSet(jobmanagerStatefulSet: V1StatefulSet?) =
        ClusterResources(
            jarUploadJob = this.jarUploadJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet
        )

    fun withTaskManagerStatefulSet(taskmanagerStatefulSet: V1StatefulSet?) =
        ClusterResources(
            jarUploadJob = this.jarUploadJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet
        )
}

