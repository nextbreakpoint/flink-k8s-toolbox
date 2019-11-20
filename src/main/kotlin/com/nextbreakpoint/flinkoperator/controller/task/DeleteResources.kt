package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class DeleteResources : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to delete resources of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val response = context.controller.deleteClusterResources(context.clusterId)

        if (!response.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry deleting resources of cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Deleting resources of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to delete resources of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        if (!resourcesHaveBeenRemoved(context.clusterId, context.resources)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for deletion of resources of cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Resources of cluster ${context.flinkCluster.metadata.name} removed in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}