package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorTimeouts

class StopJob : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        if (context.flinkCluster.spec?.flinkJob == null) {
            return Result(ResultStatus.FAILED, "Job not defined for cluster ${context.flinkCluster.metadata.name}")
        }

        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.STOPPING_JOBS_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to stop job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val response = context.controller.isJobStopped(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Job of cluster ${context.flinkCluster.metadata.name} has been stopped already")
        }

        val stopJobResponse = context.controller.stopJob(context.clusterId)

        if (stopJobResponse.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Stopping job of cluster ${context.flinkCluster.metadata.name}...")
        }

        return Result(ResultStatus.AWAIT, "Retry stopping job of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.STOPPING_JOBS_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to stop job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val response = context.controller.isJobStopped(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Job of cluster ${context.flinkCluster.metadata.name} has been stopped in ${elapsedTime / 1000} seconds")
        }

        return Result(ResultStatus.AWAIT, "Wait for termination of job of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }
}