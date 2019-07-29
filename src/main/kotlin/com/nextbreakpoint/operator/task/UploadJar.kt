package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.ResourceStatus
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.model.V1FlinkCluster
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorResources
import com.nextbreakpoint.operator.OperatorTimeouts
import com.nextbreakpoint.operator.resources.ClusterResources
import com.nextbreakpoint.operator.resources.ClusterResourcesBuilder
import com.nextbreakpoint.operator.resources.ClusterResourcesStatus
import com.nextbreakpoint.operator.resources.ClusterResourcesStatusEvaluator
import com.nextbreakpoint.operator.resources.DefaultClusterResourcesFactory
import org.apache.log4j.Logger

class UploadJar : TaskHandler {
    companion object {
        private val logger: Logger = Logger.getLogger(UploadJar::class.simpleName)
    }

    private val statusEvaluator = ClusterResourcesStatusEvaluator()

    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.UPLOADING_JAR_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to upload JAR file to cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val clusterResources = ClusterResourcesBuilder(
            DefaultClusterResourcesFactory,
            context.flinkCluster.metadata.namespace,
            context.clusterId.uuid,
            "flink-operator",
            context.flinkCluster
        ).build()

        val removeJarResponse = context.controller.removeJar(context.clusterId)

        if (removeJarResponse.status != ResultStatus.SUCCESS) {
            return Result(ResultStatus.AWAIT, "Retry removing old JAR files from cluster ${context.flinkCluster.metadata.name}...")
        }

        val uploadJarResponse = context.controller.uploadJar(context.clusterId, clusterResources)

        if (uploadJarResponse.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Uploading JAR file to cluster ${context.flinkCluster.metadata.name}...")
        }

        return Result(ResultStatus.AWAIT, "Retry uploading JAR file to cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.UPLOADING_JAR_TIMEOUT) {
            return Result(ResultStatus.FAILED, "JAR file has not been uploaded to cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)

        if (haveUploadJobResourceDiverged(clusterStatus)) {
            logger.info(clusterStatus.jarUploadJob.toString())

            return Result(ResultStatus.AWAIT, "Resources of cluster ${context.flinkCluster.metadata.name} have not been created yet")
        }

        val response = context.controller.isJarReady(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "JAR file has been uploaded to cluster ${context.flinkCluster.metadata.name} in ${elapsedTime / 1000} seconds")
        }

        return Result(ResultStatus.AWAIT, "Wait for JAR file of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    private fun evaluateClusterStatus(clusterId: ClusterId, cluster: V1FlinkCluster, resources: OperatorResources): ClusterResourcesStatus {
        val jarUploadJob = resources.jarUploadJobs.get(clusterId)
        val jobmnagerService = resources.jobmanagerServices.get(clusterId)
        val jobmanagerStatefulSet = resources.jobmanagerStatefulSets.get(clusterId)
        val taskmanagerStatefulSet = resources.taskmanagerStatefulSets.get(clusterId)

        val actualResources = ClusterResources(
            jarUploadJob = jarUploadJob,
            jobmanagerService = jobmnagerService,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet
        )

        return statusEvaluator.evaluate(clusterId, cluster, actualResources)
    }

    private fun haveUploadJobResourceDiverged(clusterResourcesStatus: ClusterResourcesStatus): Boolean {
        if (clusterResourcesStatus.jarUploadJob.first != ResourceStatus.VALID) {
            return true
        }

        return false
    }
}