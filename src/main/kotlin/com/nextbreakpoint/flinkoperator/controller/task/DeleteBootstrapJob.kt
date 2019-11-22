package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class DeleteBootstrapJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val response = context.deleteBootstrapJob(context.clusterId)

        if (!response.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry deleting bootstrap job...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Deleting bootstrap job...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        if (!bootstrapResourcesHaveBeenRemoved(context.clusterId, context.resources)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for deletion of bootstrap job...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Bootstrap job removed in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "Bootstrap job deleted")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}