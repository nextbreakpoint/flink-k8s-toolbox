package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.controller.OperatorTask
import com.nextbreakpoint.flinkoperator.controller.OperatorTimeouts
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesBuilder
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesStatus
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesStatusEvaluator
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory
import org.apache.log4j.Logger

class ReplaceResources : OperatorTask {
    companion object {
        private val logger: Logger = Logger.getLogger(ReplaceResources::class.simpleName)
    }

    private val statusEvaluator = ClusterResourcesStatusEvaluator()

    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > OperatorTimeouts.CREATING_CLUSTER_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to replace resources of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.controller.isClusterReady(context.clusterId, clusterScaling)

        if (!context.haveClusterResourcesDiverged(clusterStatus) && response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Resources of cluster ${context.flinkCluster.metadata.name} already replaced"
            )
        }

        val currentResources = context.controller.cache.getResources()

        val clusterResources = ClusterResourcesBuilder(
            DefaultClusterResourcesFactory,
            context.flinkCluster.metadata.namespace,
            context.clusterId.uuid,
            "flink-operator",
            context.flinkCluster
        ).build()

//        val jobmanagerService = currentResources.jobmanagerServices[context.clusterId]
//        clusterResources.jobmanagerService?.apiVersion = jobmanagerService?.apiVersion
//        clusterResources.jobmanagerService?.kind = jobmanagerService?.kind
//        clusterResources.jobmanagerService?.metadata = jobmanagerService?.metadata

        val jobmanagerStatefulset = currentResources.jobmanagerStatefulSets[context.clusterId]
        clusterResources.jobmanagerStatefulSet?.apiVersion = jobmanagerStatefulset?.apiVersion
        clusterResources.jobmanagerStatefulSet?.kind = jobmanagerStatefulset?.kind
        clusterResources.jobmanagerStatefulSet?.metadata = jobmanagerStatefulset?.metadata

        val taskmanagerStatefulset = currentResources.taskmanagerStatefulSets[context.clusterId]
        clusterResources.taskmanagerStatefulSet?.apiVersion = taskmanagerStatefulset?.apiVersion
        clusterResources.taskmanagerStatefulSet?.kind = taskmanagerStatefulset?.kind
        clusterResources.taskmanagerStatefulSet?.metadata = taskmanagerStatefulset?.metadata

        val replaceResponse = context.controller.replaceClusterResources(context.clusterId, clusterResources)

        if (replaceResponse.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Replacing resources of cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Retry replacing resources of cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > OperatorTimeouts.CREATING_CLUSTER_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to replace resources of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

//        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)
//
//        if (context.haveClusterResourcesDiverged(clusterStatus)) {
//            logger.info(clusterStatus.jobmanagerService.toString())
//            logger.info(clusterStatus.jobmanagerStatefulSet.toString())
//            logger.info(clusterStatus.taskmanagerStatefulSet.toString())
//
//            return Result(
//                ResultStatus.AWAIT,
//                "Wait for creation of resources of cluster ${context.flinkCluster.metadata.name}..."
//            )
//        }

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.controller.isClusterReady(context.clusterId, clusterScaling)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Resources of cluster ${context.flinkCluster.metadata.name} replaced in ${elapsedTime / 1000} seconds"
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
        val bootstrapJob = resources.bootstrapJobs.get(clusterId)
        val jobmnagerService = resources.jobmanagerServices.get(clusterId)
        val jobmanagerStatefulSet = resources.jobmanagerStatefulSets.get(clusterId)
        val taskmanagerStatefulSet = resources.taskmanagerStatefulSets.get(clusterId)

        val actualResources = ClusterResources(
            bootstrapJob = bootstrapJob,
            jobmanagerService = jobmnagerService,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet
        )

        return statusEvaluator.evaluate(clusterId, cluster, actualResources)
    }
}