package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.controller.core.Configuration
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class CancelJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CANCELLING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStoppedResponse = context.isJobStopped(context.clusterId)

        if (jobStoppedResponse.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Job is already stopped!")
        }

        val options = SavepointOptions(
            targetPath = Configuration.getSavepointTargetPath(context.flinkCluster)
        )

        val cancelJobResponse = context.cancelJob(context.clusterId, options)

        if (!cancelJobResponse.isCompleted()) {
            return taskFailedWithOutput(context.flinkCluster, "Retry cancelling job...")
        }

        val savepointRequest = cancelJobResponse.output

        if (savepointRequest == null) {
            return taskFailedWithOutput(context.flinkCluster, "Error while cancelling job")
        }

        Status.setSavepointRequest(context.flinkCluster, savepointRequest)

        return taskCompletedWithOutput(context.flinkCluster, "Cancelling job...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CANCELLING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val savepointRequest = Status.getSavepointRequest(context.flinkCluster)

        if (savepointRequest == null) {
            return taskFailedWithOutput(context.flinkCluster, "Missing savepoint request")
        }

        val jobStoppedResponse = context.isJobStopped(context.clusterId)

        if (!jobStoppedResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait until job is stopped...")
        }

        val savepointStatusResponse = context.getSavepointStatus(context.clusterId, savepointRequest)

        if (!savepointStatusResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait until savepoint is completed...")
        }

        Status.setSavepointPath(context.flinkCluster, savepointStatusResponse.output)

        return taskCompletedWithOutput(context.flinkCluster, "Job stopped in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}