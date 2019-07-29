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

        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)

        if (haveClusterResourcesDiverged(clusterStatus)) {
            logger.info(clusterStatus.jarUploadJob.toString())

            return Result(ResultStatus.AWAIT, "Resources of cluster ${context.flinkCluster.metadata.name} have not been created yet")
        }

        val jarReadyResponse = context.controller.isJarReady(context.clusterId)

        if (jarReadyResponse.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Jar has been uploaded to cluster ${context.flinkCluster.metadata.name} in ${elapsedTime / 1000} seconds")
        } else {
            return Result(ResultStatus.AWAIT, "Can't find Jar in cluster ${context.flinkCluster.metadata.name}")
        }
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
        val jobmanagerPersistentVolumeClaim = resources.jobmanagerPersistentVolumeClaims.get(clusterId)
        val taskmanagerPersistentVolumeClaim = resources.taskmanagerPersistentVolumeClaims.get(clusterId)

        val actualResources = ClusterResources(
            jarUploadJob = jarUploadJob,
            jobmanagerService = jobmnagerService,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = taskmanagerPersistentVolumeClaim
        )

        return statusEvaluator.evaluate(clusterId, cluster, actualResources)
    }

    private fun haveClusterResourcesDiverged(clusterResourcesStatus: ClusterResourcesStatus): Boolean {
        if (clusterResourcesStatus.jarUploadJob.first != ResourceStatus.VALID) {
            return true
        }

        return false
    }
}