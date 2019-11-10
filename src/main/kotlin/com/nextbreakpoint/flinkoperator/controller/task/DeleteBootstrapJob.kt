package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import com.nextbreakpoint.flinkoperator.controller.OperatorTimeouts

class DeleteBootstrapJob : OperatorTaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > OperatorTimeouts.DELETING_CLUSTER_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to delete bootstrap job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val response = context.controller.deleteBootstrapJob(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Deleting bootstrap job of cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Retry deleting bootstrap job of cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > OperatorTimeouts.DELETING_CLUSTER_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to delete bootstrap job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        if (resourcesHaveBeenRemoved(context.clusterId, context.resources)) {
            return Result(
                ResultStatus.SUCCESS,
                "Bootstrap job of cluster ${context.flinkCluster.metadata.name} removed in ${elapsedTime / 1000} seconds"
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Wait for deletion of bootstrap job of cluster ${context.flinkCluster.metadata.name}..."
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

    private fun resourcesHaveBeenRemoved(clusterId: ClusterId, resources: OperatorResources): Boolean {
        val bootstrapJob = resources.bootstrapJobs.get(clusterId)

        return bootstrapJob == null
    }
}