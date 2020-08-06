package com.nextbreakpoint.flink.k8s.controller.core

import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.common.FlinkClusterAnnotations

class ClusterContext(private val cluster: V2FlinkCluster) {
    fun setClusterWithoutSavepoint(withoutSavepoint: Boolean) {
        FlinkClusterAnnotations.setWithoutSavepoint(cluster, withoutSavepoint)
    }

    fun setClusterManualAction(action: ManualAction) {
        FlinkClusterAnnotations.setManualAction(cluster, action)
    }

    // the returned map must be immutable to avoid side effects
    fun getClusterAnnotations() = cluster.metadata?.annotations?.toMap().orEmpty()

    // we should make copy of status to avoid side effects
    fun getClusterStatus() = cluster.status
}