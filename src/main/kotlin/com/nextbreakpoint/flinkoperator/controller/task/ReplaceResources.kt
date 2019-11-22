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
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.isClusterReady(context.clusterId, clusterScaling)

        if (!context.haveClusterResourcesDiverged(clusterStatus) && response.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Resources already replaced")
        }

        val cachedResources = context.resources

        val resources = createClusterResources(context.clusterId, context.flinkCluster)

//        val jobmanagerService = currentResources.jobmanagerServices[context.clusterId]
//        clusterResources.jobmanagerService?.apiVersion = jobmanagerService?.apiVersion
//        clusterResources.jobmanagerService?.kind = jobmanagerService?.kind
//        clusterResources.jobmanagerService?.metadata = jobmanagerService?.metadata

        val jobmanagerStatefulset = cachedResources.jobmanagerStatefulSets[context.clusterId]
        resources.jobmanagerStatefulSet?.apiVersion = jobmanagerStatefulset?.apiVersion
        resources.jobmanagerStatefulSet?.kind = jobmanagerStatefulset?.kind
        resources.jobmanagerStatefulSet?.metadata = jobmanagerStatefulset?.metadata

        val taskmanagerStatefulset = cachedResources.taskmanagerStatefulSets[context.clusterId]
        resources.taskmanagerStatefulSet?.apiVersion = taskmanagerStatefulset?.apiVersion
        resources.taskmanagerStatefulSet?.kind = taskmanagerStatefulset?.kind
        resources.taskmanagerStatefulSet?.metadata = taskmanagerStatefulset?.metadata

        val replaceResourcesResponse = context.replaceClusterResources(context.clusterId, resources)

        if (!replaceResourcesResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry replacing resources...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Replacing resources...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CREATING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.isClusterReady(context.clusterId, clusterScaling)

        if (!response.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for creation...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Resources replaced in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "Resources replaced")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}