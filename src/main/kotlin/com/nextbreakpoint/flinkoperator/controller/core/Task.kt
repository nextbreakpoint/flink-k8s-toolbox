package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1BootstrapSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesBuilder
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultBootstrapJobFactory
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory
import io.kubernetes.client.models.V1Job

interface Task {
    fun onExecuting(context: TaskContext): Result<String>

    fun onAwaiting(context: TaskContext): Result<String>

    fun onIdle(context: TaskContext): Result<String>

    fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    fun isBootstrapJobDefined(cluster: V1FlinkCluster) = cluster.status?.bootstrap != null

    fun taskCompletedWithOutput(cluster: V1FlinkCluster, output: String): Result<String> =
        Result(ResultStatus.SUCCESS, "[name=${cluster.metadata.name}] $output")

    fun taskAwaitingWithOutput(cluster: V1FlinkCluster, output: String): Result<String> =
        Result(ResultStatus.AWAIT, "[name=${cluster.metadata.name}] $output")

    fun taskFailedWithOutput(cluster: V1FlinkCluster, output: String): Result<String> =
        Result(ResultStatus.FAILED, "[name=${cluster.metadata.name}] $output")

    fun makeClusterResources(clusterId: ClusterId, cluster: V1FlinkCluster): ClusterResources {
        return ClusterResourcesBuilder(
            DefaultClusterResourcesFactory,
            clusterId.namespace,
            clusterId.uuid,
            "flink-operator",
            cluster
        ).build()
    }

    fun makeBootstrapJob(clusterId: ClusterId, cluster: V1FlinkCluster): V1Job {
        return DefaultBootstrapJobFactory.createBootstrapJob(
            clusterId,
            "flink-operator",
            cluster.status.bootstrap,
            cluster.status.savepointPath,
            cluster.status.jobParallelism
        )
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

    fun computeChanges(flinkCluster: V1FlinkCluster): MutableList<String> {
        val jobManagerDigest = Status.getJobManagerDigest(flinkCluster)
        val taskManagerDigest = Status.getTaskManagerDigest(flinkCluster)
        val flinkImageDigest = Status.getRuntimeDigest(flinkCluster)
        val flinkJobDigest = Status.getBootstrapDigest(flinkCluster)

        val actualJobManagerDigest = ClusterResource.computeDigest(flinkCluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(flinkCluster.spec?.taskManager)
        val actualRuntimeDigest = ClusterResource.computeDigest(flinkCluster.spec?.runtime)
        val actualBootstrapDigest = ClusterResource.computeDigest(flinkCluster.spec?.bootstrap)

        val changes = mutableListOf<String>()

        if (jobManagerDigest != actualJobManagerDigest) {
            changes.add("JOB_MANAGER")
        }

        if (taskManagerDigest != actualTaskManagerDigest) {
            changes.add("TASK_MANAGER")
        }

        if (flinkImageDigest != actualRuntimeDigest) {
            changes.add("RUNTIME")
        }

        if (flinkJobDigest != actualBootstrapDigest) {
            changes.add("BOOTSTRAP")
        }

        return changes
    }

    fun updateDigests(flinkCluster: V1FlinkCluster) {
        val actualJobManagerDigest = ClusterResource.computeDigest(flinkCluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(flinkCluster.spec?.taskManager)
        val actualRuntimeDigest = ClusterResource.computeDigest(flinkCluster.spec?.runtime)
        val actualBootstrapDigest = ClusterResource.computeDigest(flinkCluster.spec?.bootstrap)

        Status.setJobManagerDigest(flinkCluster, actualJobManagerDigest)
        Status.setTaskManagerDigest(flinkCluster, actualTaskManagerDigest)
        Status.setRuntimeDigest(flinkCluster, actualRuntimeDigest)
        Status.setBootstrapDigest(flinkCluster, actualBootstrapDigest)
    }

    fun updateBootstrap(flinkCluster: V1FlinkCluster) {
        val bootstrap = flinkCluster.spec?.bootstrap
        Status.setBootstrap(flinkCluster, bootstrap)
    }

    fun isStartingCluster(context: TaskContext): Boolean {
        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction != ManualAction.START) {
            return false
        }

        val withoutSavepoint = Annotations.isWithSavepoint(context.flinkCluster)
        val options = StartOptions(withoutSavepoint = withoutSavepoint)
        val result = context.startCluster(context.clusterId, options)
        return result.isCompleted()
    }

    fun isStoppingCluster(context: TaskContext): Boolean {
        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction != ManualAction.STOP) {
            return false
        }

        val withoutSavepoint = Annotations.isWithSavepoint(context.flinkCluster)
        val deleteResources = Annotations.isDeleteResources(context.flinkCluster)
        val options = StopOptions(withoutSavepoint = withoutSavepoint, deleteResources = deleteResources)
        val result = context.stopCluster(context.clusterId, options)
        return result.isCompleted()
    }
}