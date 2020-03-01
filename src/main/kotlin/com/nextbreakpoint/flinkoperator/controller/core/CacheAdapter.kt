package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ManualAction

// flinkCluster property must not be exposed in order to avoid uncontrolled modifications
class CacheAdapter(private val flinkCluster: V1FlinkCluster) {
    fun setTaskManagers(taskManagers: Int) {
        Status.setTaskManagers(flinkCluster, taskManagers)
    }

    fun setTaskSlots(taskSlots: Int) {
        Status.setTaskSlots(flinkCluster, taskSlots)
    }

    fun setJobParallelism(parallelism: Int) {
        Status.setJobParallelism(flinkCluster, parallelism)
    }

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

    fun getClusterStatus() = Status.getClusterStatus(flinkCluster)

    // TODO make copy of bootstrap to avoid side effects
    fun getBootstrap() = Status.getBootstrap(flinkCluster)

    // TODO make copy of status to avoid side effects
    fun getStatus() = flinkCluster.status
}