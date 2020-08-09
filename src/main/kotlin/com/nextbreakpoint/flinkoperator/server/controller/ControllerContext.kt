package com.nextbreakpoint.flinkoperator.server.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.ManualAction
import com.nextbreakpoint.flinkoperator.server.common.Annotations

class ControllerContext(private val cluster: V1FlinkCluster) {
    fun setWithoutSavepoint(withoutSavepoint: Boolean) {
        Annotations.setWithoutSavepoint(cluster, withoutSavepoint)
    }

    fun setDeleteResources(deleteResources: Boolean) {
        Annotations.setDeleteResources(cluster, deleteResources)
    }

    fun setManualAction(action: ManualAction) {
        Annotations.setManualAction(cluster, action)
    }

    // the returned map must be immutable to avoid side effects
    fun getAnnotations() = cluster.metadata?.annotations?.toMap().orEmpty()

    // we should make copy of status to avoid side effects
    fun getStatus() = cluster.status
}