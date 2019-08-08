package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.SavepointOptions
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorParameters
import com.nextbreakpoint.operator.OperatorTimeouts

class CreateSavepoint : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        if (context.flinkCluster.spec?.flinkJob == null) {
            return Result(ResultStatus.FAILED, "Job not defined for cluster ${context.flinkCluster.metadata.name}")
        }

        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CREATING_SAVEPOINT_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val prevSavepointRequest = OperatorAnnotations.getSavepointRequest(context.flinkCluster)

        if (prevSavepointRequest != null) {
            return Result(ResultStatus.SUCCESS, "Savepoint of cluster ${context.flinkCluster.metadata.name} is already in progress...")
        }

        val options = SavepointOptions(targetPath = OperatorParameters.getSavepointTargetPath(context.flinkCluster))

        val savepointRequest = context.controller.triggerSavepoint(context.clusterId, options)

        if (savepointRequest.status == ResultStatus.SUCCESS && savepointRequest.output != null) {
            OperatorAnnotations.setSavepointRequest(context.flinkCluster, savepointRequest.output)

            return Result(ResultStatus.SUCCESS, "Creating savepoint of cluster ${context.flinkCluster.metadata.name}...")
        }

        return Result(ResultStatus.AWAIT, "Retry creating savepoint of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CREATING_SAVEPOINT_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val savepointRequest = OperatorAnnotations.getSavepointRequest(context.flinkCluster)

        if (savepointRequest == null) {
            return Result(ResultStatus.FAILED, "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val completedSavepoint = context.controller.getSavepointStatus(context.clusterId, savepointRequest)

        if (completedSavepoint.status == ResultStatus.SUCCESS) {
            if (OperatorParameters.getSavepointPath(context.flinkCluster) != completedSavepoint.output) {
                OperatorAnnotations.setSavepointPath(context.flinkCluster, completedSavepoint.output)
            }

            return Result(ResultStatus.SUCCESS, "Savepoint of cluster ${context.flinkCluster.metadata.name} has been created")
        }

        return Result(ResultStatus.AWAIT, "Wait for completion of savepoint of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }
}