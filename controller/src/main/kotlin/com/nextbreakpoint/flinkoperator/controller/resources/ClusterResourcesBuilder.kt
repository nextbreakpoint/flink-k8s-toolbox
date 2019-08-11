package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster

class ClusterResourcesBuilder(
    private val factory: ClusterResourcesFactory,
    private val namespace: String,
    private val clusterId: String,
    private val clusterOwner: String,
    private val flinkCluster: V1FlinkCluster
) {
    fun build(): ClusterResources {
        val jobmanagerService = factory.createJobManagerService(
            namespace, clusterId, clusterOwner, flinkCluster
        )

        val jobmanagerStatefulSet = factory.createJobManagerStatefulSet(
            namespace, clusterId, clusterOwner, flinkCluster
        )

        val taskmanagerStatefulSet = factory.createTaskManagerStatefulSet(
            namespace, clusterId, clusterOwner, flinkCluster
        )

        val jarUploadJob = factory.createJarUploadJob(
            namespace, clusterId, clusterOwner, flinkCluster
        )

        return ClusterResources(
            jarUploadJob = jarUploadJob,
            jobmanagerService = jobmanagerService,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet
        )
    }
}