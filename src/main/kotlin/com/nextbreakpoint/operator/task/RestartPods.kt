package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorTimeouts
import com.nextbreakpoint.operator.resources.ClusterResourcesBuilder
import com.nextbreakpoint.operator.resources.DefaultClusterResourcesFactory

class RestartPods : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.TERMINATING_PODS_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to restart pods of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val clusterResources = ClusterResourcesBuilder(
            DefaultClusterResourcesFactory,
            context.flinkCluster.metadata.namespace,
            context.clusterId.uuid,
            "flink-operator",
            context.flinkCluster
        ).build()

        val response = context.controller.restartPods(context.clusterId, clusterResources)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Restarting pods of cluster ${context.flinkCluster.metadata.name}...")
        }

        return Result(ResultStatus.AWAIT, "Retry restarting pods of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.TERMINATING_PODS_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to restart pods of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val response = context.controller.isClusterReady(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Resources of cluster ${context.flinkCluster.metadata.name} have been restarted in ${elapsedTime / 1000} seconds")
        }

        return Result(ResultStatus.AWAIT, "Wait for creation of pods of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }
}