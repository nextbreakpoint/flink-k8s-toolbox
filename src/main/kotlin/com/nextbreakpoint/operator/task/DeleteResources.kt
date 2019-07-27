package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorResources
import com.nextbreakpoint.operator.OperatorTimeouts

class DeleteResources : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.DELETING_CLUSTER_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to delete resources of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val response = context.controller.deleteClusterResources(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Deleting resources of cluster ${context.flinkCluster.metadata.name}...")
        } else {
            return Result(ResultStatus.AWAIT, "Can't delete resources of cluster ${context.flinkCluster.metadata.name}")
        }
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.DELETING_CLUSTER_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to delete resources of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        if (resourcesHaveBeenRemoved(context.clusterId, context.resources)) {
            return Result(ResultStatus.SUCCESS, "Resources of cluster ${context.flinkCluster.metadata.name} have been removed")
        } else {
            return Result(ResultStatus.AWAIT, "Failed to delete resources of cluster ${context.flinkCluster.metadata.name}")
        }
    }

    override fun onIdle(context: OperatorContext) {
    }

    override fun onFailed(context: OperatorContext) {
    }

    private fun resourcesHaveBeenRemoved(clusterId: ClusterId, resources: OperatorResources): Boolean {
        val jarUploadJob = resources.jarUploadJobs.get(clusterId)
        val jobmnagerService = resources.jobmanagerServices.get(clusterId)
        val jobmanagerStatefulSet = resources.jobmanagerStatefulSets.get(clusterId)
        val taskmanagerStatefulSet = resources.taskmanagerStatefulSets.get(clusterId)
        val jobmanagerPersistentVolumeClaim = resources.jobmanagerPersistentVolumeClaims.get(clusterId)
        val taskmanagerPersistentVolumeClaim = resources.taskmanagerPersistentVolumeClaims.get(clusterId)

        return jarUploadJob == null &&
                jobmnagerService == null &&
                jobmanagerStatefulSet == null &&
                taskmanagerStatefulSet == null &&
                jobmanagerPersistentVolumeClaim == null &&
                taskmanagerPersistentVolumeClaim == null
    }
}