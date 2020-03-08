package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

data class CachedResources(
    val flinkCluster: V1FlinkCluster? = null,
    val bootstrapJob: V1Job? = null,
    val jobmanagerService: V1Service? = null,
    val jobmanagerStatefulSet: V1StatefulSet? = null,
    val taskmanagerStatefulSet: V1StatefulSet? = null,
    val jobmanagerPVC: V1PersistentVolumeClaim? = null,
    val taskmanagerPVC: V1PersistentVolumeClaim? = null
) {
    fun withBootstrap(resource: V1Job?) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            bootstrapJob = resource,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPVC = this.jobmanagerPVC,
            taskmanagerPVC = this.taskmanagerPVC
        )

    fun withJobManagerService(resource: V1Service?) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            bootstrapJob = this.bootstrapJob,
            jobmanagerService = resource,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPVC = this.jobmanagerPVC,
            taskmanagerPVC = this.taskmanagerPVC
        )

    fun withJobManagerStatefulSet(resource: V1StatefulSet?) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            bootstrapJob = this.bootstrapJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = resource,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPVC = this.jobmanagerPVC,
            taskmanagerPVC = this.taskmanagerPVC
        )

    fun withTaskManagerStatefulSet(resource: V1StatefulSet?) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            bootstrapJob = this.bootstrapJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = resource,
            jobmanagerPVC = this.jobmanagerPVC,
            taskmanagerPVC = this.taskmanagerPVC
        )

    fun withJobManagerPVC(resource: V1PersistentVolumeClaim?) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            bootstrapJob = this.bootstrapJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPVC = resource,
            taskmanagerPVC = this.taskmanagerPVC
        )

    fun withTaskManagerPVC(resource: V1PersistentVolumeClaim?) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            bootstrapJob = this.bootstrapJob,
            jobmanagerService = this.jobmanagerService,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPVC = this.jobmanagerPVC,
            taskmanagerPVC = resource
        )
}