package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service

data class ClusterResources(
    val flinkCluster: V2FlinkCluster? = null,
    val flinkJobs: Map<String, V1FlinkJob> = mapOf(),
    val jobmanagerPods: Set<V1Pod> = setOf(),
    val taskmanagerPods: Set<V1Pod> = setOf(),
    val service: V1Service? = null
) {
    fun withService(resource: V1Service?) =
        ClusterResources(
            flinkCluster = this.flinkCluster,
            jobmanagerPods = this.jobmanagerPods,
            taskmanagerPods = this.taskmanagerPods,
            service = resource,
            flinkJobs = this.flinkJobs
        )

    fun withJobManagerPods(resources: Set<V1Pod>) =
        ClusterResources(
            flinkCluster = this.flinkCluster,
            jobmanagerPods = resources,
            taskmanagerPods = this.taskmanagerPods,
            service = this.service,
            flinkJobs = this.flinkJobs
        )

    fun withTaskManagerPods(resources: Set<V1Pod>) =
        ClusterResources(
            flinkCluster = this.flinkCluster,
            jobmanagerPods = this.jobmanagerPods,
            taskmanagerPods = resources,
            service = this.service,
            flinkJobs = this.flinkJobs
        )

    fun withFlinkJobs(resources: Set<V1FlinkJob>) =
        ClusterResources(
            flinkCluster = this.flinkCluster,
            jobmanagerPods = this.jobmanagerPods,
            taskmanagerPods = this.taskmanagerPods,
            service = this.service,
            flinkJobs = flinkJobs
        )
}