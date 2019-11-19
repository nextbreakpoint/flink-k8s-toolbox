package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class DeleteBootstrapJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to delete bootstrap job of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val response = context.controller.deleteBootstrapJob(context.clusterId)

        if (response.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Deleting bootstrap job of cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Retry deleting bootstrap job of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to delete bootstrap job of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        if (resourcesHaveBeenRemoved(context.clusterId, context.resources)) {
            return taskCompletedWithOutput(context.flinkCluster, "Bootstrap job of cluster ${context.flinkCluster.metadata.name} removed in $seconds seconds")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Wait for deletion of bootstrap job of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    private fun resourcesHaveBeenRemoved(clusterId: ClusterId, resources: CachedResources): Boolean {
        val bootstrapJob = resources.bootstrapJobs.get(clusterId)

        return bootstrapJob == null
    }
}