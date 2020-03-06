package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ManualAction

class CacheAdapter(private val flinkCluster: V1FlinkCluster) {
    fun setWithoutSavepoint(withoutSavepoint: Boolean) {
        Annotations.setWithoutSavepoint(flinkCluster, withoutSavepoint)
    }

    fun setDeleteResources(deleteResources: Boolean) {
        Annotations.setDeleteResources(flinkCluster, deleteResources)
    }

    fun setManualAction(action: ManualAction) {
        Annotations.setManualAction(flinkCluster, action)
    }

    // the returned map must be immutable to avoid side effects
    fun getAnnotations() = flinkCluster.metadata?.annotations?.toMap().orEmpty()

    // we should make copy of status to avoid side effects
    fun getStatus() = flinkCluster.status
}