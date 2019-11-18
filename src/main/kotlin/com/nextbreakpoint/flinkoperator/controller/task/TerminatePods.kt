package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class TerminatePods : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.TERMINATING_RESOURCES_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to terminate pods of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val response = context.controller.terminatePods(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Terminating pods of cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Retry terminating pods of cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.TERMINATING_RESOURCES_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to terminate pods of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val response = context.controller.arePodsTerminated(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Resources of cluster ${context.flinkCluster.metadata.name} terminated in ${elapsedTime / 1000} seconds"
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Wait for termination of pods of cluster ${context.flinkCluster.metadata.name}..."
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