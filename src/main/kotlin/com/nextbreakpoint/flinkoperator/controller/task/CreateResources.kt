package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesBuilder
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesStatus
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesValidator
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory

class CreateResources : Task {
    private val validator = ClusterResourcesValidator()

    override fun onExecuting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.CREATING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to create resources of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.controller.isClusterReady(context.clusterId, clusterScaling)

        if (!context.haveClusterResourcesDiverged(clusterStatus) && response.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Resources of cluster ${context.flinkCluster.metadata.name} already created")
        }

        val clusterResources = ClusterResourcesBuilder(
            DefaultClusterResourcesFactory,
            context.flinkCluster.metadata.namespace,
            context.clusterId.uuid,
            "flink-operator",
            context.flinkCluster
        ).build()

        val createResponse = context.controller.createClusterResources(context.clusterId, clusterResources)

        if (createResponse.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Creating resources of cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Retry creating resources of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.CREATING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to create resources of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val clusterScale = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.controller.isClusterReady(context.clusterId, clusterScale)

        if (response.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Resources of cluster ${context.flinkCluster.metadata.name} created in $seconds seconds")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Wait for creation of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    private fun evaluateClusterStatus(clusterId: ClusterId, cluster: V1FlinkCluster, resources: CachedResources): ClusterResourcesStatus {
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

        return validator.evaluate(clusterId, cluster, actualResources)
    }
}