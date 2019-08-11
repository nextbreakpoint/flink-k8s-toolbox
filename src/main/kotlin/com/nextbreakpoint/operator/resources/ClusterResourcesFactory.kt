package com.nextbreakpoint.operator.resources

import com.nextbreakpoint.model.V1FlinkCluster
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

interface ClusterResourcesFactory {
    fun createJobManagerService(
        namespace: String,
        clusterId: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1Service?

    fun createJarUploadJob(
        namespace: String,
        clusterId: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1Job?

    fun createJobManagerStatefulSet(
        namespace: String,
        clusterId: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1StatefulSet?

    fun createTaskManagerStatefulSet(
        namespace: String,
        clusterId: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1StatefulSet?
}