package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Configuration
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class CancelJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        if (context.flinkCluster.spec?.bootstrap == null) {
            return Result(
                ResultStatus.FAILED,
                "Cluster ${context.flinkCluster.metadata.name} doesn't have a job"
            )
        }

        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.CANCELLING_JOB_TIMEOUT) {
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
            targetPath = Configuration.getSavepointTargetPath(context.flinkCluster)
        )

        val savepointRequest = context.controller.cancelJob(context.clusterId, options)

        if (savepointRequest.status == ResultStatus.SUCCESS && savepointRequest.output != null) {
            Status.setSavepointRequest(context.flinkCluster, savepointRequest.output)

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

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.CANCELLING_JOB_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to cancel job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val savepointRequest = Status.getSavepointRequest(context.flinkCluster)

        if (savepointRequest == null) {
            return Result(
                ResultStatus.FAILED,
                "Failed to cancel job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val completedSavepoint = context.controller.getSavepointStatus(context.clusterId, savepointRequest)

        if (completedSavepoint.status == ResultStatus.SUCCESS) {
            if (Configuration.getSavepointPath(context.flinkCluster) != completedSavepoint.output) {
                Status.setSavepointPath(context.flinkCluster, completedSavepoint.output)
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