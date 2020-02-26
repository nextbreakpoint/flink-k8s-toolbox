package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.controller.core.Configuration
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class TriggerSavepoint : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CREATING_SAVEPOINT_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobRunningResponse = context.isJobRunning(context.clusterId)

        if (!jobRunningResponse.isCompleted()) {
            return fail(context.flinkCluster, "Job not running!")
        }

        val options = SavepointOptions(
            targetPath = Configuration.getSavepointTargetPath(context.flinkCluster)
        )

        val response = context.triggerSavepoint(context.clusterId, options)

        if (!response.isCompleted()) {
            return repeat(context.flinkCluster, "Retry creating savepoint...")
        }

        val savepointRequest = response.output

        Status.setSavepointRequest(context.flinkCluster, savepointRequest)

        return next(context.flinkCluster, "Creating savepoint...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CREATING_SAVEPOINT_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val savepointRequest = Status.getSavepointRequest(context.flinkCluster)

        if (savepointRequest == null) {
            return fail(context.flinkCluster, "Failed to create savepoint after $seconds seconds")
        }

        val savepointStatusResponse = context.getLatestSavepoint(context.clusterId, savepointRequest)

        if (!savepointStatusResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Creating savepoint...")
        }

        val savepointPath = savepointStatusResponse.output

        Status.setSavepointPath(context.flinkCluster, savepointPath)

        return next(context.flinkCluster, "Savepoint created after $seconds seconds")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Savepoint created")
    }
}