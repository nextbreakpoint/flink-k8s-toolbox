package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import io.kubernetes.client.models.V1Pod
import io.kubernetes.client.models.V1Service

interface ClusterResourcesFactory {
    fun createService(
        namespace: String,
        clusterSelector: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1Service

    fun createJobManagerPod(
        namespace: String,
        clusterSelector: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1Pod

    fun createTaskManagerPod(
        namespace: String,
        clusterSelector: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1Pod
}