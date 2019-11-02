package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorParameters
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import com.nextbreakpoint.flinkoperator.controller.OperatorTimeouts

class CancelJob : OperatorTaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        if (context.flinkCluster.spec?.flinkJob == null) {
            return Result(
                ResultStatus.FAILED,
                "Cluster ${context.flinkCluster.metadata.name} doesn't have a job"
            )
        }

        val elapsedTime = context.controller.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CANCELLING_JOBS_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to cancel job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val response = context.controller.isJobStopped(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Job of cluster ${context.flinkCluster.metadata.name} already stopped"
            )
        }

        val options = SavepointOptions(
            targetPath = OperatorParameters.getSavepointTargetPath(context.flinkCluster)
        )

        val savepointRequest = context.controller.cancelJob(context.clusterId, options)

        if (savepointRequest.status == ResultStatus.SUCCESS && savepointRequest.output != null) {
            OperatorState.setSavepointRequest(context.flinkCluster, savepointRequest.output)

            return Result(
                ResultStatus.SUCCESS,
                "Cancelling job of cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Retry cancelling job of cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CANCELLING_JOBS_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to cancel job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val savepointRequest = OperatorState.getSavepointRequest(context.flinkCluster)

        if (savepointRequest == null) {
            return Result(
                ResultStatus.FAILED,
                "Failed to cancel job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val completedSavepoint = context.controller.getSavepointStatus(context.clusterId, savepointRequest)

        if (completedSavepoint.status == ResultStatus.SUCCESS) {
            if (OperatorParameters.getSavepointPath(context.flinkCluster) != completedSavepoint.output) {
                OperatorState.setSavepointPath(context.flinkCluster, completedSavepoint.output)
            }
        }

        if (completedSavepoint.status == ResultStatus.AWAIT) {
            return Result(
                ResultStatus.AWAIT,
                "Wait for savepoint of job of cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        val response = context.controller.isJobStopped(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Job of cluster ${context.flinkCluster.metadata.name} stopped in ${elapsedTime / 1000} seconds"
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Wait for termination of job of cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }
}