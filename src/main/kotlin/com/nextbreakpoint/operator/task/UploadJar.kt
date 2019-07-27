package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorTimeouts
import com.nextbreakpoint.operator.resources.ClusterResourcesBuilder
import com.nextbreakpoint.operator.resources.DefaultClusterResourcesFactory

class UploadJar : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.UPLOADING_JAR_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to upload Jar to cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val clusterResources = ClusterResourcesBuilder(
            DefaultClusterResourcesFactory,
            context.flinkCluster.metadata.namespace,
            context.clusterId.uuid,
            "flink-operator",
            context.flinkCluster
        ).build()

        val removeJarsResponse = context.controller.removeJar(context.clusterId)

        if (removeJarsResponse.status != ResultStatus.SUCCESS) {
            return Result(ResultStatus.AWAIT, "Can't remove old jars from cluster ${context.flinkCluster.metadata.name}")
        }

        val uploadJarResponse = context.controller.uploadJar(context.clusterId, clusterResources)

        if (uploadJarResponse.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Uploading Jar to cluster ${context.flinkCluster.metadata.name}...")
        } else {
            return Result(ResultStatus.AWAIT, "Can't upload Jar to cluster ${context.flinkCluster.metadata.name}")
        }
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.UPLOADING_JAR_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Jar has not been uploaded to cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val jarReadyResponse = context.controller.isJarReady(context.clusterId)

        if (jarReadyResponse.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Jar has been uploaded to cluster ${context.flinkCluster.metadata.name} in ${elapsedTime / 1000} seconds")
        } else {
            return Result(ResultStatus.AWAIT, "Can't find Jar in cluster ${context.flinkCluster.metadata.name}")
        }
    }

    override fun onIdle(context: OperatorContext) {
    }

    override fun onFailed(context: OperatorContext) {
    }
}