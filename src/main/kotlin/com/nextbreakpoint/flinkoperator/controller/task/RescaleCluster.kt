package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class RescaleCluster : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.RESCALING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val desiredTaskManagers = Status.getTaskManagers(context.flinkCluster)

        val result = context.setTaskManagersReplicas(context.clusterId, desiredTaskManagers)

        if (!result.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Can't rescale task managers")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Task managers have been rescaled")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.RESCALING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val result = context.getTaskManagersReplicas(context.clusterId)

        if (!result.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Task managers have not been scaled yet...")
        }

        val desiredTaskManagers = Status.getTaskManagers(context.flinkCluster)

        if (desiredTaskManagers != result.output) {
            return taskAwaitingWithOutput(context.flinkCluster, "Task managers have not been scaled yet...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Task managers have been scaled")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}