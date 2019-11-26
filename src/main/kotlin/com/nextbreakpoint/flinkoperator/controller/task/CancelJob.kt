package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.controller.core.Configuration
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class CancelJob : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CANCELLING_JOB_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStoppedResponse = context.isJobStopped(context.clusterId)

        if (jobStoppedResponse.isCompleted()) {
            return skip(context.flinkCluster, "Job already stopped")
        }

        val options = SavepointOptions(
            targetPath = Configuration.getSavepointTargetPath(context.flinkCluster)
        )

        val cancelJobResponse = context.cancelJob(context.clusterId, options)

        if (!cancelJobResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Retry cancelling job...")
        }

        val savepointRequest = cancelJobResponse.output

        Status.setSavepointRequest(context.flinkCluster, savepointRequest)

        return next(context.flinkCluster, "Cancelling job...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CANCELLING_JOB_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val savepointRequest = Status.getSavepointRequest(context.flinkCluster)

        if (savepointRequest == null) {
            return fail(context.flinkCluster, "Missing savepoint request")
        }

        val jobStoppedResponse = context.isJobStopped(context.clusterId)

        if (!jobStoppedResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Cancelling job...")
        }

        val lastestSavepointResponse = context.getLatestSavepoint(context.clusterId, savepointRequest)

        if (!lastestSavepointResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Savepoint not created yet...")
        }

        val savepointPath = lastestSavepointResponse.output

        Status.setSavepointPath(context.flinkCluster, savepointPath)

        return next(context.flinkCluster, "Job stopped after $seconds seconds")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Job cancelled")
    }
}