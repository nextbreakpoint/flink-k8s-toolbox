package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service

data class ClusterResources(
    val flinkCluster: V1FlinkCluster? = null,
    val jobmanagerPods: Set<V1Pod> = setOf(),
    val taskmanagerPods: Set<V1Pod> = setOf(),
    val jobmanagerService: V1Service? = null,
    val flinkJobs: Set<V1FlinkJob> = setOf()
) {
    fun withService(resource: V1Service?) =
        ClusterResources(
            flinkCluster = this.flinkCluster,
            jobmanagerPods = this.jobmanagerPods,
            taskmanagerPods = this.taskmanagerPods,
            jobmanagerService = resource,
            flinkJobs = this.flinkJobs
        )
}