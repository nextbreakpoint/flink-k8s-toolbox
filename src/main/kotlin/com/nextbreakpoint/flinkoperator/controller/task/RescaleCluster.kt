package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class RescaleCluster : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.RESCALING_CLUSTER_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val desiredTaskManagers = Status.getTaskManagers(context.flinkCluster)

        val result = context.setTaskManagersReplicas(context.clusterId, desiredTaskManagers)

        if (!result.isCompleted()) {
            return repeat(context.flinkCluster, "Retry rescaling task managers...")
        }

        return next(context.flinkCluster, "Rescaling task managers...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.RESCALING_CLUSTER_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val result = context.getTaskManagersReplicas(context.clusterId)

        if (!result.isCompleted()) {
            return repeat(context.flinkCluster, "Rescaling task managers...")
        }

        val desiredTaskManagers = Status.getTaskManagers(context.flinkCluster)

        if (desiredTaskManagers != result.output) {
            return repeat(context.flinkCluster, "Rescaling task managers...")
        }

        return next(context.flinkCluster, "Task managers rescaled")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Cluster rescaled")
    }
}