package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

interface ClusterResourcesFactory {
    fun createJobManagerService(
        namespace: String,
        clusterSelector: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1Service

    fun createJobManagerStatefulSet(
        namespace: String,
        clusterSelector: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1StatefulSet

    fun createTaskManagerStatefulSet(
        namespace: String,
        clusterSelector: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1StatefulSet
}