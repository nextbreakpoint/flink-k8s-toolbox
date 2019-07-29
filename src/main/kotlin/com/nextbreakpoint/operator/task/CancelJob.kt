package com.nextbreakpoint.operator.task

import com.google.gson.Gson
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.SavepointOptions
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorTimeouts

class CancelJob : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CANCELLING_JOBS_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to cancel job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val response = context.controller.isJobStopped(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Job of cluster ${context.flinkCluster.metadata.name} has been stopped already")
        }

        val options = SavepointOptions(targetPath = context.flinkCluster.spec?.flinkOperator?.targetPath)

        val savepointRequest = context.controller.cancelJob(context.clusterId, options)

        if (savepointRequest.status == ResultStatus.SUCCESS) {
            OperatorAnnotations.setSavepointRequest(context.flinkCluster, Gson().toJson(savepointRequest.output))

            return Result(ResultStatus.SUCCESS, "Cancelling job of cluster ${context.flinkCluster.metadata.name}...")
        }

        return Result(ResultStatus.AWAIT, "Retry cancelling job of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CANCELLING_JOBS_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to cancel job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val requests = OperatorAnnotations.getSavepointRequest(context.flinkCluster) ?: "{}"

        val savepointRequest = Gson().fromJson<Map<String, String>>(requests, Map::class.java)

        val completedSavepoint = context.controller.getSavepointStatus(context.clusterId, savepointRequest)

        if (completedSavepoint.status == ResultStatus.SUCCESS) {
            if (OperatorAnnotations.getSavepointPath(context.flinkCluster) != completedSavepoint.output) {
                OperatorAnnotations.setSavepointPath(context.flinkCluster, completedSavepoint.output)
            }
        }

        if (completedSavepoint.status == ResultStatus.AWAIT) {
            return Result(ResultStatus.AWAIT, "Wait for savepoint of job of cluster ${context.flinkCluster.metadata.name}...")
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