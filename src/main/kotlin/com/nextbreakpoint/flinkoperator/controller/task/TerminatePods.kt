package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class TerminatePods : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TERMINATING_RESOURCES_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val response = context.terminatePods(context.clusterId)

        if (!response.isCompleted()) {
            return repeat(context.flinkCluster, "Retry terminating pods...")
        }

        return next(context.flinkCluster, "Terminating pods...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TERMINATING_RESOURCES_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val response = context.arePodsTerminated(context.clusterId)

        if (!response.isCompleted()) {
            return repeat(context.flinkCluster, "Terminating pods...")
        }

        return next(context.flinkCluster, "Resources terminated after $seconds seconds")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Pods terminated")
    }
}