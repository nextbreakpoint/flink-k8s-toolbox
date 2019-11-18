package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class StopJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        if (context.flinkCluster.spec?.bootstrap == null) {
            return Result(
                ResultStatus.FAILED,
                "Cluster ${context.flinkCluster.metadata.name} doesn't have a job"
            )
        }

        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.STOPPING_JOB_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to stop job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val response = context.controller.isJobStopped(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Job of cluster ${context.flinkCluster.metadata.name} already stopped"
            )
        }

        val stopJobResponse = context.controller.stopJob(context.clusterId)

        if (stopJobResponse.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Stopping job of cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Retry stopping job of cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.STOPPING_JOB_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to stop job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
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