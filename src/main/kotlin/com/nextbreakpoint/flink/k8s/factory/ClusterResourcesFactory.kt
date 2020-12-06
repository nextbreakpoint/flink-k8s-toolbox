package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service

interface ClusterResourcesFactory {
    fun createService(
        namespace: String,
        owner: String,
        clusterName: String,
        clusterSpec: V1FlinkClusterSpec
    ): V1Service

    fun createJobManagerPod(
        namespace: String,
        owner: String,
        clusterName: String,
        clusterSpec: V1FlinkClusterSpec
    ): V1Pod

    fun createTaskManagerPod(
        namespace: String,
        owner: String,
        clusterName: String,
        clusterSpec: V1FlinkClusterSpec
    ): V1Pod
}