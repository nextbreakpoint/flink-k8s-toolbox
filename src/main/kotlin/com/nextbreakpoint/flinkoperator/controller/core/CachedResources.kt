package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Pod
import io.kubernetes.client.models.V1Service

data class CachedResources(
    val flinkCluster: V1FlinkCluster? = null,
    val bootstrapJob: V1Job? = null,
    val jobmanagerPods: Set<V1Pod> = setOf(),
    val taskmanagerPods: Set<V1Pod> = setOf(),
    val service: V1Service? = null
) {
    fun withBootstrapJob(resource: V1Job?) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            bootstrapJob = resource,
            jobmanagerPods = this.jobmanagerPods,
            taskmanagerPods = this.taskmanagerPods,
            service = this.service
        )

    fun withService(resource: V1Service?) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            bootstrapJob = this.bootstrapJob,
            jobmanagerPods = this.jobmanagerPods,
            taskmanagerPods = this.taskmanagerPods,
            service = resource
        )

    fun withJobManagerPods(resources: Set<V1Pod>) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            bootstrapJob = this.bootstrapJob,
            jobmanagerPods = resources,
            taskmanagerPods = this.taskmanagerPods,
            service = this.service
        )

    fun withTaskManagerPods(resources: Set<V1Pod>) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            bootstrapJob = this.bootstrapJob,
            jobmanagerPods = this.jobmanagerPods,
            taskmanagerPods = resources,
            service = this.service
        )
}