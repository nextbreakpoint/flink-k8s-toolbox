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
        val jobmanagerService = factory.createJobManagerService(
            namespace, clusterSelector, clusterOwner, flinkCluster
        )

        val jobmanagerStatefulSet = factory.createJobManagerStatefulSet(
            namespace, clusterSelector, clusterOwner, flinkCluster
        )

        val taskmanagerStatefulSet = factory.createTaskManagerStatefulSet(
            namespace, clusterSelector, clusterOwner, flinkCluster
        )

        return ClusterResources(
            jobmanagerService = jobmanagerService,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet
        )
    }
}