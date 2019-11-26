package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class DeleteBootstrapJob : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val response = context.deleteBootstrapJob(context.clusterId)

        if (!response.isCompleted()) {
            return repeat(context.flinkCluster, "Retry deleting bootstrap job...")
        }

        return next(context.flinkCluster, "Deleting bootstrap job...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        if (!bootstrapResourcesHaveBeenRemoved(context.clusterId, context.resources)) {
            return repeat(context.flinkCluster, "Deleting bootstrap job...")
        }

        return next(context.flinkCluster, "Bootstrap job removed after $seconds seconds")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Bootstrap job deleted")
    }
}