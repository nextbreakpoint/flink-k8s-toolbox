package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class TerminatePods : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TERMINATING_RESOURCES_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val response = context.terminatePods(context.clusterId)

        if (!response.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry terminating pods...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Terminating pods...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TERMINATING_RESOURCES_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val response = context.arePodsTerminated(context.clusterId)

        if (!response.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for termination of pods...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Resources terminated in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "Pods terminated")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}