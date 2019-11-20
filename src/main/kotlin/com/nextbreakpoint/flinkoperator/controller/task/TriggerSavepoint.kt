package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.controller.core.Configuration
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class TriggerSavepoint : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CREATING_SAVEPOINT_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobRunningResponse = context.controller.isJobRunning(context.clusterId)

        if (!jobRunningResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry creating savepoint...")
        }

        val options = SavepointOptions(
            targetPath = Configuration.getSavepointTargetPath(context.flinkCluster)
        )

        val response = context.controller.triggerSavepoint(context.clusterId, options)

        if (!response.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry creating savepoint of cluster ${context.flinkCluster.metadata.name}...")
        }

        val savepointRequest = response.output

        if (savepointRequest == null) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry creating savepoint of cluster ${context.flinkCluster.metadata.name}...")
        }

        Status.setSavepointRequest(context.flinkCluster, savepointRequest)

        return taskCompletedWithOutput(context.flinkCluster, "Creating savepoint of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CREATING_SAVEPOINT_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val savepointRequest = Status.getSavepointRequest(context.flinkCluster)

        if (savepointRequest == null) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val savepointStatusResponse = context.controller.getSavepointStatus(context.clusterId, savepointRequest)

        if (!savepointStatusResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for completion of savepoint of cluster ${context.flinkCluster.metadata.name}...")
        }

        val savepointPath = savepointStatusResponse.output

        if (savepointPath == null) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for completion of savepoint of cluster ${context.flinkCluster.metadata.name}...")
        }

        Status.setSavepointPath(context.flinkCluster, savepointPath)

        return taskCompletedWithOutput(context.flinkCluster, "Savepoint of cluster ${context.flinkCluster.metadata.name} created in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}