package com.nextbreakpoint.flink.k8s.controller.core

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.common.FlinkClusterAnnotations
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster

class ClusterContext(private val cluster: V1FlinkCluster) {
    fun setClusterWithoutSavepoint(withoutSavepoint: Boolean) {
        FlinkClusterAnnotations.setWithoutSavepoint(cluster, withoutSavepoint)
    }

    fun setClusterManualAction(action: Action) {
        FlinkClusterAnnotations.setRequestedAction(cluster, action)
    }

    // the returned map must be immutable to avoid side effects
    fun getClusterAnnotations() = cluster.metadata?.annotations?.toMap().orEmpty()

    // we should make copy of status to avoid side effects
    fun getClusterStatus() = cluster.status
}