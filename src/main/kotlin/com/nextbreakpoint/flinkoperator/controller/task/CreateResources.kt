package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ResourceStatus
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import com.nextbreakpoint.flinkoperator.controller.OperatorTimeouts
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesBuilder
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesStatus
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesStatusEvaluator
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory
import org.apache.log4j.Logger

class CreateResources : OperatorTaskHandler {
    companion object {
        private val logger: Logger = Logger.getLogger(CreateResources::class.simpleName)
    }

    private val statusEvaluator = ClusterResourcesStatusEvaluator()

    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CREATING_CLUSTER_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to create resources of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)

        val response = context.controller.isClusterReady(context.clusterId)

        if (!context.haveClusterResourcesDiverged(clusterStatus) && response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Resources of cluster ${context.flinkCluster.metadata.name} already created"
            )
        }

        val clusterResources = ClusterResourcesBuilder(
            DefaultClusterResourcesFactory,
            context.flinkCluster.metadata.namespace,
            context.clusterId.uuid,
            "flink-operator",
            context.flinkCluster
        ).build()

        val createResponse = context.controller.createClusterResources(context.clusterId, clusterResources)

        if (createResponse.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Creating resources of cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Retry creating resources of cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CREATING_CLUSTER_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to create resources of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)

        if (context.haveClusterResourcesDiverged(clusterStatus)) {
            logger.info(clusterStatus.jobmanagerService.toString())
            logger.info(clusterStatus.jobmanagerStatefulSet.toString())
            logger.info(clusterStatus.taskmanagerStatefulSet.toString())

            return Result(
                ResultStatus.AWAIT,
                "Wait for creation of resources of cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        val response = context.controller.isClusterReady(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Resources of cluster ${context.flinkCluster.metadata.name} created in ${elapsedTime / 1000} seconds"
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Wait for creation of cluster ${context.flinkCluster.metadata.name}..."
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
}