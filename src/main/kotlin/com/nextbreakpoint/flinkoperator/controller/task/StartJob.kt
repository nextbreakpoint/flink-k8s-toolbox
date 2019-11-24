package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class StartJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return taskCompletedWithOutput(context.flinkCluster, "Bootstrap job not defined")
        }

        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STARTING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStartedResponse = context.isJobStarted(context.clusterId)

        if (jobStartedResponse.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Job already started")
        }

        val startJobResponse = context.startJob(context.clusterId, context.flinkCluster)

        if (!startJobResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry starting job...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Starting job...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return taskCompletedWithOutput(context.flinkCluster, "Bootstrap job not defined")
        }

        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STARTING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStartedResponse = context.isJobStarted(context.clusterId)

        if (!jobStartedResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Starting job...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Job started in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "Job started")
    }
}