package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ManualAction

// flinkCluster property must not be exposed in order to avoid uncontrolled modifications
class CacheAdapter(private val flinkCluster: V1FlinkCluster, val cacheResources: CachedResources) {
    fun setTaskManagers(taskManagers: Int) {
        Status.setTaskManagers(flinkCluster, taskManagers)
    }

    fun setTaskSlots(taskSlots: Int) {
        Status.setTaskSlots(flinkCluster, taskSlots)
    }

    fun setJobParallelism(parallelism: Int) {
        Status.setJobParallelism(flinkCluster, parallelism)
    }

    fun appendTasks(tasks: List<ClusterTask>) {
        Status.appendTasks(flinkCluster, tasks)
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

    // the returned map must be immutable to avoid side effects
    fun getStatus() = mapOf(
        "timestamp" to (flinkCluster.status?.timestamp?.toString() ?: ""),
        "clusterStatus" to (flinkCluster.status?.clusterStatus ?: ""),
        "taskStatus" to (flinkCluster.status?.taskStatus ?: ""),
        "tasks" to (flinkCluster.status?.tasks?.joinToString(" ") ?: ""),
        "taskAttempts" to (flinkCluster.status?.taskAttempts?.toString() ?: ""),
        "savepointPath" to (flinkCluster.status?.savepointPath ?: ""),
        "savepointTimestamp" to (flinkCluster.status?.savepointTimestamp?.toString() ?: ""),
        "savepointJobId" to (flinkCluster.status?.savepointJobId ?: ""),
        "savepointTriggerId" to (flinkCluster.status?.savepointTriggerId ?: ""),
        "runtimeDigest" to (flinkCluster.status?.digest?.runtime ?: ""),
        "bootstrapDigest" to (flinkCluster.status?.digest?.bootstrap ?: ""),
        "jobManagerDigest" to (flinkCluster.status?.digest?.jobManager ?: ""),
        "taskManagerDigest" to (flinkCluster.status?.digest?.taskManager ?: "")
    )
}