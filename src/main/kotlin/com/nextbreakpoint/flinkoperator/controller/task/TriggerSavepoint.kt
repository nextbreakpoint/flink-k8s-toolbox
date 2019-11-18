package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Configuration
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class TriggerSavepoint : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        if (context.flinkCluster.spec?.bootstrap == null) {
            return Result(
                ResultStatus.FAILED,
                "Cluster ${context.flinkCluster.metadata.name} doesn't have a job"
            )
        }

        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.CREATING_SAVEPOINT_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val prevSavepointRequest = Status.getSavepointRequest(context.flinkCluster)

        if (prevSavepointRequest != null) {
            return Result(
                ResultStatus.SUCCESS,
                "Savepoint of cluster ${context.flinkCluster.metadata.name} is already in progress..."
            )
        }

        val options = SavepointOptions(
            targetPath = Configuration.getSavepointTargetPath(context.flinkCluster)
        )

        val savepointRequest = context.controller.triggerSavepoint(context.clusterId, options)

        if (savepointRequest.status == ResultStatus.SUCCESS && savepointRequest.output != null) {
            Status.setSavepointRequest(context.flinkCluster, savepointRequest.output)

            return Result(
                ResultStatus.SUCCESS,
                "Creating savepoint of cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Retry creating savepoint of cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.CREATING_SAVEPOINT_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val savepointRequest = Status.getSavepointRequest(context.flinkCluster)

        if (savepointRequest == null) {
            return Result(
                ResultStatus.FAILED,
                "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val completedSavepoint = context.controller.getSavepointStatus(context.clusterId, savepointRequest)

        if (completedSavepoint.status == ResultStatus.SUCCESS) {
            if (Configuration.getSavepointPath(context.flinkCluster) != completedSavepoint.output) {
                Status.setSavepointPath(context.flinkCluster, completedSavepoint.output)
            }

            return Result(
                ResultStatus.SUCCESS,
                "Savepoint of cluster ${context.flinkCluster.metadata.name} created in ${elapsedTime / 1000} seconds"
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Wait for completion of savepoint of cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }
}