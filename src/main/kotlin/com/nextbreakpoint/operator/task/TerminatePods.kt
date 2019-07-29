package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorTimeouts

class TerminatePods : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.DELETING_CLUSTER_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to terminate pods of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val response = context.controller.terminatePods(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Terminating pods of cluster ${context.flinkCluster.metadata.name}...")
        } else {
            return Result(ResultStatus.AWAIT, "Can't terminate pods of cluster ${context.flinkCluster.metadata.name}")
        }
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.DELETING_CLUSTER_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to terminate pods of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val response = context.controller.arePodsTerminated(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Resources of cluster ${context.flinkCluster.metadata.name} have been terminated")
        } else {
            return Result(ResultStatus.AWAIT, "Failed to check pods of cluster ${context.flinkCluster.metadata.name}")
        }
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }
}