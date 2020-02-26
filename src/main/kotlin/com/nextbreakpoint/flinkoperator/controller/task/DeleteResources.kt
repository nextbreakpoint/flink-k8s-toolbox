package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class DeleteResources : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val response = context.deleteClusterResources(context.clusterId)

        if (!response.isCompleted()) {
            return repeat(context.flinkCluster, "Retry deleting resources...")
        }

        return next(context.flinkCluster, "Deleting resources...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        if (!resourcesHaveBeenRemoved(context.clusterId, context.resources)) {
            return repeat(context.flinkCluster, "Deleting resources...")
        }

        return next(context.flinkCluster, "Resources removed after $seconds seconds")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Resources deleted")
    }
}