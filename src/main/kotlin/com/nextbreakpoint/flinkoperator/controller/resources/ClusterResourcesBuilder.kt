package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster

class ClusterResourcesBuilder(
    private val factory: ClusterResourcesFactory,
    private val namespace: String,
    private val clusterSelector: String,
    private val clusterOwner: String,
    private val flinkCluster: V1FlinkCluster
) {
    fun build(): ClusterResources {
        val service = factory.createService(
            namespace, clusterSelector, clusterOwner, flinkCluster
        )

        val jobmanagerPod = factory.createJobManagerPod(
            namespace, clusterSelector, clusterOwner, flinkCluster
        )

        val taskmanagerPod = factory.createTaskManagerPod(
            namespace, clusterSelector, clusterOwner, flinkCluster
        )

        return ClusterResources(
            service = service,
            jobmanagerPod = jobmanagerPod,
            taskmanagerPod = taskmanagerPod
        )
    }
}