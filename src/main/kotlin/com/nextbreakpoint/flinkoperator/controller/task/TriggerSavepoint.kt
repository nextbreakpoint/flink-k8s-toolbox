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
        if (context.flinkCluster.spec?.bootstrap == null) {
            return taskFailedWithOutput(context.flinkCluster, "Cluster ${context.flinkCluster.metadata.name} doesn't have a job")
        }

        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.CREATING_SAVEPOINT_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val savepointRequest = Status.getSavepointRequest(context.flinkCluster)

        if (savepointRequest != null) {
            return taskCompletedWithOutput(context.flinkCluster, "Savepoint of cluster ${context.flinkCluster.metadata.name} is already in progress...")
        }

        val options = SavepointOptions(
            targetPath = Configuration.getSavepointTargetPath(context.flinkCluster)
        )

        val response = context.controller.triggerSavepoint(context.clusterId, options)

        if (!response.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry creating savepoint of cluster ${context.flinkCluster.metadata.name}...")
        }

        if (response.output == null) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry creating savepoint of cluster ${context.flinkCluster.metadata.name}...")
        }

        Status.setSavepointRequest(context.flinkCluster, response.output)

        return taskCompletedWithOutput(context.flinkCluster, "Creating savepoint of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.CREATING_SAVEPOINT_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val savepointRequest = Status.getSavepointRequest(context.flinkCluster)

        if (savepointRequest == null) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val completedSavepoint = context.controller.getSavepointStatus(context.clusterId, savepointRequest)

        if (!completedSavepoint.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for completion of savepoint of cluster ${context.flinkCluster.metadata.name}...")
        }

        Status.setSavepointPath(context.flinkCluster, completedSavepoint.output)

        return taskCompletedWithOutput(context.flinkCluster, "Savepoint of cluster ${context.flinkCluster.metadata.name} created in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}