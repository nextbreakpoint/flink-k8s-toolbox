package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class StartJob : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return skip(context.flinkCluster, "Bootstrap job not defined")
        }

        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STARTING_JOB_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStartedResponse = context.isJobStarted(context.clusterId)

        if (jobStartedResponse.isCompleted()) {
            return skip(context.flinkCluster, "Job already started")
        }

        val startJobResponse = context.startJob(context.clusterId, context.flinkCluster)

        if (!startJobResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Retry starting job...")
        }

        return next(context.flinkCluster, "Starting job...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return skip(context.flinkCluster, "Bootstrap job not defined")
        }

        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STARTING_JOB_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStartedResponse = context.isJobStarted(context.clusterId)

        if (!jobStartedResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Starting job...")
        }

        return next(context.flinkCluster, "Job started after $seconds seconds")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Job started")
    }
}