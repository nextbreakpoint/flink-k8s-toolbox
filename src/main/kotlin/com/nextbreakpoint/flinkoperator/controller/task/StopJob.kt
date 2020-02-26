package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class StopJob : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STOPPING_JOB_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStoppedResponse = context.isJobStopped(context.clusterId)

        if (jobStoppedResponse.isCompleted()) {
            return skip(context.flinkCluster, "Job already stopped")
        }

        val stopJobResponse = context.stopJob(context.clusterId)

        if (!stopJobResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Retry stopping job...")
        }

        return next(context.flinkCluster, "Stopping job...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STOPPING_JOB_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStoppedResponse = context.isJobStopped(context.clusterId)

        if (!jobStoppedResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Stopping job...")
        }

        return next(context.flinkCluster, "Job stopped after $seconds seconds")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Job stopped")
    }
}