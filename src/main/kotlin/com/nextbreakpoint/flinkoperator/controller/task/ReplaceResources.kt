package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class ReplaceResources : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CREATING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to replace resources of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.controller.isClusterReady(context.clusterId, clusterScaling)

        if (!context.haveClusterResourcesDiverged(clusterStatus) && response.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Resources of cluster ${context.flinkCluster.metadata.name} already replaced")
        }

        val currentResources = context.controller.cache.getResources()

        val resources = createClusterResources(context.clusterId, context.flinkCluster)

//        val jobmanagerService = currentResources.jobmanagerServices[context.clusterId]
//        clusterResources.jobmanagerService?.apiVersion = jobmanagerService?.apiVersion
//        clusterResources.jobmanagerService?.kind = jobmanagerService?.kind
//        clusterResources.jobmanagerService?.metadata = jobmanagerService?.metadata

        val jobmanagerStatefulset = currentResources.jobmanagerStatefulSets[context.clusterId]
        resources.jobmanagerStatefulSet?.apiVersion = jobmanagerStatefulset?.apiVersion
        resources.jobmanagerStatefulSet?.kind = jobmanagerStatefulset?.kind
        resources.jobmanagerStatefulSet?.metadata = jobmanagerStatefulset?.metadata

        val taskmanagerStatefulset = currentResources.taskmanagerStatefulSets[context.clusterId]
        resources.taskmanagerStatefulSet?.apiVersion = taskmanagerStatefulset?.apiVersion
        resources.taskmanagerStatefulSet?.kind = taskmanagerStatefulset?.kind
        resources.taskmanagerStatefulSet?.metadata = taskmanagerStatefulset?.metadata

        val replaceResourcesResponse = context.controller.replaceClusterResources(context.clusterId, resources)

        if (!replaceResourcesResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry replacing resources of cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Replacing resources of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CREATING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to replace resources of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.controller.isClusterReady(context.clusterId, clusterScaling)

        if (!response.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for creation of cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Resources of cluster ${context.flinkCluster.metadata.name} replaced in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}