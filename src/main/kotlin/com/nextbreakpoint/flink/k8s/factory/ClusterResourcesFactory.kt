package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service

interface ClusterResourcesFactory {
    fun createService(
        namespace: String,
        clusterSelector: String,
        clusterOwner: String,
        flinkCluster: V2FlinkCluster
    ): V1Service

    fun createJobManagerPod(
        namespace: String,
        clusterSelector: String,
        clusterOwner: String,
        flinkCluster: V2FlinkCluster
    ): V1Pod

    fun createTaskManagerPod(
        namespace: String,
        clusterSelector: String,
        clusterOwner: String,
        flinkCluster: V2FlinkCluster
    ): V1Pod
}