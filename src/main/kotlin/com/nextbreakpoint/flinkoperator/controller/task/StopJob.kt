package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class StopJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STOPPING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStoppedResponse = context.isJobStopped(context.clusterId)

        if (jobStoppedResponse.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Job already stopped")
        }

        val stopJobResponse = context.stopJob(context.clusterId)

        if (!stopJobResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry stopping job...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Stopping job...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STOPPING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStoppedResponse = context.isJobStopped(context.clusterId)

        if (!jobStoppedResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for termination of job...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Job stopped in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "Job stopped")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}