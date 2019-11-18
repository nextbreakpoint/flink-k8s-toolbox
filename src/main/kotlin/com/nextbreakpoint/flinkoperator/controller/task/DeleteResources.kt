package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class DeleteResources : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to delete resources of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val response = context.controller.deleteClusterResources(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Deleting resources of cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Retry deleting resources of cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.DELETING_CLUSTER_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to delete resources of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        if (resourcesHaveBeenRemoved(context.clusterId, context.resources)) {
            return Result(
                ResultStatus.SUCCESS,
                "Resources of cluster ${context.flinkCluster.metadata.name} removed in ${elapsedTime / 1000} seconds"
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Wait for deletion of resources of cluster ${context.flinkCluster.metadata.name}..."
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

    private fun resourcesHaveBeenRemoved(clusterId: ClusterId, resources: CachedResources): Boolean {
        val bootstrapJob = resources.bootstrapJobs.get(clusterId)
        val jobmnagerService = resources.jobmanagerServices.get(clusterId)
        val jobmanagerStatefulSet = resources.jobmanagerStatefulSets.get(clusterId)
        val taskmanagerStatefulSet = resources.taskmanagerStatefulSets.get(clusterId)
        val jobmanagerPersistentVolumeClaim = resources.jobmanagerPersistentVolumeClaims.get(clusterId)
        val taskmanagerPersistentVolumeClaim = resources.taskmanagerPersistentVolumeClaims.get(clusterId)

        return bootstrapJob == null &&
                jobmnagerService == null &&
                jobmanagerStatefulSet == null &&
                taskmanagerStatefulSet == null &&
                jobmanagerPersistentVolumeClaim == null &&
                taskmanagerPersistentVolumeClaim == null
    }
}