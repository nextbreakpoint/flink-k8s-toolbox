package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorResources
import com.nextbreakpoint.operator.OperatorTimeouts

class DeleteUploadJob : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.DELETING_CLUSTER_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to delete upload job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val response = context.controller.deleteUploadJobResource(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Deleting upload job of cluster ${context.flinkCluster.metadata.name}...")
        }

        return Result(ResultStatus.AWAIT, "Retry deleting upload job of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.DELETING_CLUSTER_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to delete upload job of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        if (resourcesHaveBeenRemoved(context.clusterId, context.resources)) {
            return Result(ResultStatus.SUCCESS, "Upload job of cluster ${context.flinkCluster.metadata.name} has been removed in ${elapsedTime / 1000} seconds")
        }

        return Result(ResultStatus.AWAIT, "Wait for deletion of upload job of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    private fun resourcesHaveBeenRemoved(clusterId: ClusterId, resources: OperatorResources): Boolean {
        val jarUploadJob = resources.jarUploadJobs.get(clusterId)

        return jarUploadJob == null
    }
}