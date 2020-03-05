package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

data class CachedResources(
    val flinkCluter: V1FlinkCluster? = null,
    val bootstrapJob: V1Job? = null,
    val jobmanagerService: V1Service? = null,
    val jobmanagerStatefulSet: V1StatefulSet? = null,
    val taskmanagerStatefulSet: V1StatefulSet? = null,
    val jobmanagerPVC: V1PersistentVolumeClaim? = null,
    val taskmanagerPVC: V1PersistentVolumeClaim? = null
) {
    fun withFlinkCluster(flinkCluter: V1FlinkCluster?) =
            CachedResources(
                    flinkCluter = flinkCluter,
                    bootstrapJob = this.bootstrapJob,
                    jobmanagerService = this.jobmanagerService,
                    jobmanagerStatefulSet = this.jobmanagerStatefulSet,
                    taskmanagerStatefulSet = this.taskmanagerStatefulSet,
                    jobmanagerPVC = this.jobmanagerPVC,
                    taskmanagerPVC = this.taskmanagerPVC
            )

    fun withBootstrapJob(bootstrapJob: V1Job?) =
            CachedResources(
                    flinkCluter = this.flinkCluter,
                    bootstrapJob = bootstrapJob,
                    jobmanagerService = this.jobmanagerService,
                    jobmanagerStatefulSet = this.jobmanagerStatefulSet,
                    taskmanagerStatefulSet = this.taskmanagerStatefulSet,
                    jobmanagerPVC = this.jobmanagerPVC,
                    taskmanagerPVC = this.taskmanagerPVC
            )

    fun withJobManagerService(jobmanagerService: V1Service?) =
            CachedResources(
                    flinkCluter = this.flinkCluter,
                    bootstrapJob = this.bootstrapJob,
                    jobmanagerService = jobmanagerService,
                    jobmanagerStatefulSet = this.jobmanagerStatefulSet,
                    taskmanagerStatefulSet = this.taskmanagerStatefulSet,
                    jobmanagerPVC = this.jobmanagerPVC,
                    taskmanagerPVC = this.taskmanagerPVC
            )

    fun withJobManagerStatefulSet(jobmanagerStatefulSet: V1StatefulSet?) =
            CachedResources(
                    flinkCluter = this.flinkCluter,
                    bootstrapJob = this.bootstrapJob,
                    jobmanagerService = this.jobmanagerService,
                    jobmanagerStatefulSet = jobmanagerStatefulSet,
                    taskmanagerStatefulSet = this.taskmanagerStatefulSet,
                    jobmanagerPVC = this.jobmanagerPVC,
                    taskmanagerPVC = this.taskmanagerPVC
            )

    fun withTaskManagerStatefulSet(taskmanagerStatefulSet: V1StatefulSet?) =
            CachedResources(
                    flinkCluter = this.flinkCluter,
                    bootstrapJob = this.bootstrapJob,
                    jobmanagerService = this.jobmanagerService,
                    jobmanagerStatefulSet = this.jobmanagerStatefulSet,
                    taskmanagerStatefulSet = taskmanagerStatefulSet,
                    jobmanagerPVC = this.jobmanagerPVC,
                    taskmanagerPVC = this.taskmanagerPVC
            )

    fun withJobManagerPVC(jobmanagerPVC: V1PersistentVolumeClaim?) =
            CachedResources(
                    flinkCluter = this.flinkCluter,
                    bootstrapJob = this.bootstrapJob,
                    jobmanagerService = this.jobmanagerService,
                    jobmanagerStatefulSet = jobmanagerStatefulSet,
                    taskmanagerStatefulSet = this.taskmanagerStatefulSet,
                    jobmanagerPVC = jobmanagerPVC,
                    taskmanagerPVC = this.taskmanagerPVC
            )

    fun withTaskManagerPVC(taskmanagerPVC: V1PersistentVolumeClaim?) =
            CachedResources(
                    flinkCluter = this.flinkCluter,
                    bootstrapJob = this.bootstrapJob,
                    jobmanagerService = this.jobmanagerService,
                    jobmanagerStatefulSet = this.jobmanagerStatefulSet,
                    taskmanagerStatefulSet = taskmanagerStatefulSet,
                    jobmanagerPVC = this.jobmanagerPVC,
                    taskmanagerPVC = taskmanagerPVC
            )
}