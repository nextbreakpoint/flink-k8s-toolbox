package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesBuilder
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultBootstrapJobFactory
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory
import io.kubernetes.client.models.V1Job

interface Task {
    fun onExecuting(context: TaskContext): TaskResult<String>

    fun onAwaiting(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Nothing to do")
    }

    fun onFailed(context: TaskContext): TaskResult<String> {
        return repeat(context.flinkCluster, "Not healthy")
    }

    fun onIdle(context: TaskContext): TaskResult<String> {
        return repeat(context.flinkCluster, "Idle")
    }

    fun isBootstrapJobDefined(cluster: V1FlinkCluster) = cluster.status?.bootstrap != null

    fun repeat(cluster: V1FlinkCluster, output: String): TaskResult<String> =
        TaskResult(TaskAction.REPEAT, "[name=${cluster.metadata.name}] $output")

    fun next(cluster: V1FlinkCluster, output: String): TaskResult<String> =
        TaskResult(TaskAction.NEXT, "[name=${cluster.metadata.name}] $output")

    fun skip(cluster: V1FlinkCluster, output: String): TaskResult<String> =
        TaskResult(TaskAction.SKIP, "[name=${cluster.metadata.name}] $output")

    fun fail(cluster: V1FlinkCluster, output: String): TaskResult<String> =
        TaskResult(TaskAction.FAIL, "[name=${cluster.metadata.name}] $output")

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
        val bootstrapJob = resources.bootstrapJobs[clusterId]
        val jobmnagerService = resources.jobmanagerServices[clusterId]
        val jobmanagerStatefulSet = resources.jobmanagerStatefulSets[clusterId]
        val taskmanagerStatefulSet = resources.taskmanagerStatefulSets[clusterId]
        val jobmanagerPersistentVolumeClaim = resources.jobmanagerPersistentVolumeClaims[clusterId]
        val taskmanagerPersistentVolumeClaim = resources.taskmanagerPersistentVolumeClaims[clusterId]

        return bootstrapJob == null &&
                jobmnagerService == null &&
                jobmanagerStatefulSet == null &&
                taskmanagerStatefulSet == null &&
                jobmanagerPersistentVolumeClaim == null &&
                taskmanagerPersistentVolumeClaim == null
    }

    fun bootstrapResourcesHaveBeenRemoved(clusterId: ClusterId, resources: CachedResources): Boolean {
        val bootstrapJob = resources.bootstrapJobs[clusterId]

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