package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesBuilder
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesStatus
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesValidator
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory

interface Task {
    fun onExecuting(context: TaskContext): Result<String>

    fun onAwaiting(context: TaskContext): Result<String>

    fun onIdle(context: TaskContext): Result<String>

    fun onFailed(context: TaskContext): Result<String>

    fun isBootstrapJobDefined(cluster: V1FlinkCluster) = cluster.spec?.bootstrap != null

    fun taskCompletedWithOutput(cluster: V1FlinkCluster, output: String): Result<String> =
        Result(ResultStatus.SUCCESS, "[name=${cluster.metadata.name}] $output")

    fun taskAwaitingWithOutput(cluster: V1FlinkCluster, output: String): Result<String> =
        Result(ResultStatus.AWAIT, "[name=${cluster.metadata.name}] $output")

    fun taskFailedWithOutput(cluster: V1FlinkCluster, output: String): Result<String> =
        Result(ResultStatus.FAILED, "[name=${cluster.metadata.name}] $output")

    fun createClusterResources(clusterId: ClusterId, cluster: V1FlinkCluster): ClusterResources {
        return ClusterResourcesBuilder(
            DefaultClusterResourcesFactory, clusterId.namespace, clusterId.uuid, "flink-operator", cluster
        ).build()
    }

    fun evaluateClusterStatus(clusterId: ClusterId, cluster: V1FlinkCluster, resources: CachedResources): ClusterResourcesStatus {
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

        return ClusterResourcesValidator().evaluate(clusterId, cluster, actualResources)
    }

    fun resourcesHaveBeenRemoved(clusterId: ClusterId, resources: CachedResources): Boolean {
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

    fun bootstrapResourcesHaveBeenRemoved(clusterId: ClusterId, resources: CachedResources): Boolean {
        val bootstrapJob = resources.bootstrapJobs.get(clusterId)

        return bootstrapJob == null
    }

    fun secondsSinceLastUpdate(context: TaskContext) = (context.controller.currentTimeMillis() - context.operatorTimestamp) / 1000
}