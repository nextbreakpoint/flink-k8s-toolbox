package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class DeleteResources : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val response = context.deleteClusterResources(context.clusterId)

        if (!response.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry deleting resources...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Deleting resources...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        if (!resourcesHaveBeenRemoved(context.clusterId, context.resources)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for resources...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Resources removed in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "Resources deleted")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}