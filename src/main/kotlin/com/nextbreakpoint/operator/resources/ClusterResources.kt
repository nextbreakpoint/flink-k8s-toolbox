package com.nextbreakpoint.operator.resources

import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

data class ClusterResources(
    val jarUploadJob: V1Job?,
    val jobmanagerService: V1Service?,
    val jobmanagerStatefulSet: V1StatefulSet?,
    val taskmanagerStatefulSet: V1StatefulSet?,
    val jobmanagerPersistentVolumeClaim: V1PersistentVolumeClaim?,
    val taskmanagerPersistentVolumeClaim: V1PersistentVolumeClaim?
) {
    fun withJobManagerService(jobmanagerService: V1Service?) =
        ClusterResources(
            jarUploadJob = this.jarUploadJob,
            jobmanagerService = jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = this.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = this.taskmanagerPersistentVolumeClaim
        )

    fun withJarUploadJob(jarUploadJob: V1Job?) =
        ClusterResources(
            jarUploadJob = jarUploadJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = this.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = this.taskmanagerPersistentVolumeClaim
        )

    fun withJobManagerStatefulSet(jobmanagerStatefulSet: V1StatefulSet?) =
        ClusterResources(
            jarUploadJob = this.jarUploadJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = this.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = this.taskmanagerPersistentVolumeClaim
        )

    fun withTaskManagerStatefulSet(taskmanagerStatefulSet: V1StatefulSet?) =
        ClusterResources(
            jarUploadJob = this.jarUploadJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = this.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = this.taskmanagerPersistentVolumeClaim
        )

    fun withJobManagerPersistenVolumeClaim(jobmanagerPersistentVolumeClaim: V1PersistentVolumeClaim?) =
        ClusterResources(
            jarUploadJob = this.jarUploadJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = this.taskmanagerPersistentVolumeClaim
        )

    fun withTaskManagerPersistenVolumeClaim(taskmanagerPersistentVolumeClaim: V1PersistentVolumeClaim?) =
        ClusterResources(
            jarUploadJob = this.jarUploadJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = this.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = taskmanagerPersistentVolumeClaim
        )
}

