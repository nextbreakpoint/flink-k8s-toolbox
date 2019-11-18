package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import org.apache.log4j.Logger

class RescaleCluster : Task {
    companion object {
        private val logger = Logger.getLogger(RescaleCluster::class.simpleName)
    }

    override fun onExecuting(context: TaskContext): Result<String> {
        try {
            val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

            if (elapsedTime > Timeout.RESCALING_CLUSTER_TIMEOUT) {
                return Result(
                    ResultStatus.FAILED,
                    "Failed to rescale task managers of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
                )
            }

            val desiredTaskManagers = Status.getTaskManagers(context.flinkCluster)

            val result = context.controller.setTaskManagersReplicas(context.clusterId, desiredTaskManagers)

            if (result.status != ResultStatus.SUCCESS) {
                return Result(
                    ResultStatus.AWAIT,
                    "Can't rescale task managers of cluster ${context.clusterId.name}"
                )
            }

            return Result(
                ResultStatus.SUCCESS,
                "Task managers of cluster ${context.clusterId.name} have been rescaled"
            )
        } catch (e : Exception) {
            logger.error("Can't rescale task managers of cluster ${context.clusterId.name}")

            return Result(
                ResultStatus.AWAIT,
                "Failed to rescale task managers of cluster ${context.clusterId.name}"
            )
        }
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        try {
            val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

            if (elapsedTime > Timeout.RESCALING_CLUSTER_TIMEOUT) {
                return Result(
                    ResultStatus.FAILED,
                    "Failed to scale task managers of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
                )
            }

            val result = context.controller.getTaskManagersReplicas(context.clusterId)

            if (result.status != ResultStatus.SUCCESS) {
                return Result(
                    ResultStatus.AWAIT,
                    "Task managers of cluster ${context.clusterId.name} have not been scaled yet..."
                )
            }

            val desiredTaskManagers = Status.getTaskManagers(context.flinkCluster)

            if (desiredTaskManagers != result.output) {
                return Result(
                    ResultStatus.AWAIT,
                    "Task managers of cluster ${context.clusterId.name} have not been scaled yet..."
                )
            }

            return Result(
                ResultStatus.SUCCESS,
                "Task managers of cluster ${context.clusterId.name} have been scaled"
            )
        } catch (e : Exception) {
            logger.error("Can't scale task managers of cluster ${context.clusterId.name}")

            return Result(
                ResultStatus.AWAIT,
                "Failed to scale task managers of cluster ${context.clusterId.name}"
            )
        }
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