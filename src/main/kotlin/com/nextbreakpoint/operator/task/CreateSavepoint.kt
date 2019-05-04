package com.nextbreakpoint.operator.task

import com.google.gson.Gson
import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorTimeouts

class CreateSavepoint : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CREATING_SAVEPOINT_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val savepointRequest = context.controller.triggerSavepoint(context.clusterId)

        if (savepointRequest.status == ResultStatus.SUCCESS) {
            OperatorAnnotations.setSavepointRequest(context.flinkCluster, Gson().toJson(savepointRequest.output))

            return Result(ResultStatus.SUCCESS, "Creating savepoint of cluster ${context.flinkCluster.metadata.name}...")
        } else {
            return Result(ResultStatus.AWAIT, "Can't create savepoint of cluster ${context.flinkCluster.metadata.name}")
        }
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CREATING_SAVEPOINT_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to create savepoint of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val requests = OperatorAnnotations.getSavepointRequest(context.flinkCluster) ?: return Result(ResultStatus.FAILED, "Savepoint request not found in cluster ${context.flinkCluster.metadata.name}")

        val savepointRequest = Gson().fromJson<Map<String, String>>(requests, Map::class.java)

        val completedSavepoint = context.controller.getSavepointStatus(context.clusterId, savepointRequest)

        if (completedSavepoint.status == ResultStatus.SUCCESS) {
            OperatorAnnotations.setSavepointPath(context.flinkCluster, completedSavepoint.output)

            return Result(ResultStatus.SUCCESS, "Savepoint of cluster ${context.flinkCluster.metadata.name} has been created")
        } else {
            return Result(ResultStatus.AWAIT, "Failed to check savepoint status of cluster ${context.flinkCluster.metadata.name}")
        }
    }

    override fun onIdle(context: OperatorContext) {
    }

    override fun onFailed(context: OperatorContext) {
    }
}