package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultBootstrapJobFactory
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import org.joda.time.DateTime

class TaskContext(
    val clusterId: ClusterId,
    private val cluster: V1FlinkCluster,
    private val resources: CachedResources,
    private val controller: OperationController
) {
    fun timeSinceLastUpdateInSeconds() = (controller.currentTimeMillis() - Status.getStatusTimestamp(cluster).millis) / 1000L

    fun timeSinceLastSavepointRequestInSeconds() = (controller.currentTimeMillis() - Status.getSavepointRequestTimestamp(cluster).millis) / 1000L

    fun removeJar(clusterId: ClusterId) : OperationResult<Void?> =
        controller.removeJar(clusterId)

    fun triggerSavepoint(clusterId: ClusterId, options: SavepointOptions) : OperationResult<SavepointRequest> =
        controller.triggerSavepoint(clusterId, options)

    fun getLatestSavepoint(clusterId: ClusterId, savepointRequest: SavepointRequest) : OperationResult<String> =
        controller.getLatestSavepoint(clusterId, savepointRequest)

    fun createBootstrapJob(clusterId: ClusterId, bootstrapJob: V1Job): OperationResult<String?> =
        controller.createBootstrapJob(clusterId, bootstrapJob)

    fun deleteBootstrapJob(clusterId: ClusterId) : OperationResult<Void?> =
        controller.deleteBootstrapJob(clusterId)

    fun terminatePods(clusterId: ClusterId) : OperationResult<Void?> =
        controller.terminatePods(clusterId)

    fun restartPods(clusterId: ClusterId, options: ClusterScaling): OperationResult<Void?> =
        controller.restartPods(clusterId, options)

    fun arePodsTerminated(clusterId: ClusterId): OperationResult<Void?> =
        controller.arePodsTerminated(clusterId)

    fun startJob(clusterId: ClusterId, cluster: V1FlinkCluster) : OperationResult<Void?> =
        controller.startJob(clusterId, cluster)

    fun stopJob(clusterId: ClusterId): OperationResult<Void?> =
        controller.stopJob(clusterId)

    fun cancelJob(clusterId: ClusterId, options: SavepointOptions): OperationResult<SavepointRequest> =
        controller.cancelJob(clusterId, options)

    fun isClusterReady(clusterId: ClusterId, options: ClusterScaling): OperationResult<Void?> =
        controller.isClusterReady(clusterId, options)

    fun isJobRunning(clusterId: ClusterId): OperationResult<Void?> =
        controller.isJobRunning(clusterId)

    fun isJobFinished(clusterId: ClusterId): OperationResult<Void?> =
        controller.isJobFinished(clusterId)

    fun createJobManagerService(clusterId: ClusterId, service: V1Service): OperationResult<String?> =
        controller.createJobManagerService(clusterId, service)

    fun deleteJobManagerService(clusterId: ClusterId): OperationResult<Void?> =
        controller.deleteJobManagerService(clusterId)

    fun createStatefulSet(clusterId: ClusterId, statefulSet: V1StatefulSet): OperationResult<String?> =
        controller.createStatefulSet(clusterId, statefulSet)

    fun deleteStatefulSets(clusterId: ClusterId): OperationResult<Void?> =
        controller.deleteStatefulSets(clusterId)

    fun deletePersistentVolumeClaims(clusterId: ClusterId): OperationResult<Void?> =
        controller.deletePersistentVolumeClaims(clusterId)

    fun refreshStatus(statusTimestamp: DateTime, actionTimestamp: DateTime, hasFinalizer: Boolean) {
        val taskManagers = resources.taskmanagerStatefulSet?.status?.readyReplicas ?: 0
        if (Status.getActiveTaskManagers(cluster) != taskManagers) {
            Status.setActiveTaskManagers(cluster, taskManagers)
        }

        val taskSlots = cluster.status?.taskSlots ?: 1
        if (Status.getTotalTaskSlots(cluster) != taskManagers * taskSlots) {
            Status.setTotalTaskSlots(cluster, taskManagers * taskSlots)
        }

        val savepointMode = cluster.spec?.operator?.savepointMode
        if (Status.getSavepointMode(cluster) != savepointMode) {
            Status.setSavepointMode(cluster, savepointMode)
        }

        val jobRestartPolicy = cluster.spec?.operator?.jobRestartPolicy
        if (Status.getJobRestartPolicy(cluster) != jobRestartPolicy) {
            Status.setJobRestartPolicy(cluster, jobRestartPolicy)
        }

        val newStatusTimestamp = Status.getStatusTimestamp(cluster)

        if (statusTimestamp != newStatusTimestamp) {
            controller.updateStatus(clusterId, cluster)
        }

        val newActionTimestamp = Annotations.getActionTimestamp(cluster)

        if (actionTimestamp != newActionTimestamp) {
            controller.updateAnnotations(clusterId, cluster)
        }

        val newHasFinalizer = hasFinalizer()

        if (hasFinalizer != newHasFinalizer) {
            controller.updateFinalizers(clusterId, cluster)
        }
    }

    fun hasBeenDeleted(): Boolean = cluster.metadata.deletionTimestamp != null

    fun hasFinalizer(): Boolean = cluster.metadata.finalizers.orEmpty().contains("finalizer.nextbreakpoint.com")

    fun addFinalizer() {
        if (cluster.metadata.finalizers == null) {
            cluster.metadata.finalizers = listOf()
        }

        if (!cluster.metadata.finalizers.contains("finalizer.nextbreakpoint.com")) {
            cluster.metadata.finalizers = cluster.metadata.finalizers.plus("finalizer.nextbreakpoint.com")
        }
    }

    fun removeFinalizer() {
        if (cluster.metadata.finalizers == null) {
            return
        }

        if (cluster.metadata.finalizers.contains("finalizer.nextbreakpoint.com")) {
            cluster.metadata.finalizers = cluster.metadata.finalizers.minus("finalizer.nextbreakpoint.com")
        }
    }

    fun initializeStatus() {
        val bootstrap = cluster.spec?.bootstrap
        Status.setBootstrap(cluster, bootstrap)

        val taskManagers = cluster.spec?.taskManagers ?: 0
        val taskSlots = cluster.spec?.taskManager?.taskSlots ?: 1
        Status.setTaskManagers(cluster, taskManagers)
        Status.setTaskSlots(cluster, taskSlots)
        Status.setJobParallelism(cluster, taskManagers * taskSlots)

        val savepointPath = cluster.spec?.operator?.savepointPath
        Status.setSavepointPath(cluster, savepointPath ?: "")

        val labelSelector = ClusterResource.makeLabelSelector(clusterId)
        Status.setLabelSelector(cluster, labelSelector)

        val serviceMode = cluster.spec?.jobManager?.serviceMode
        Status.setServiceMode(cluster, serviceMode)

        val savepointMode = cluster.spec?.operator?.savepointMode
        Status.setSavepointMode(cluster, savepointMode)

        val jobRestartPolicy = cluster.spec?.operator?.jobRestartPolicy
        Status.setJobRestartPolicy(cluster, jobRestartPolicy)

        Status.setTaskStatus(cluster, TaskStatus.Idle)
    }

    fun initializeAnnotations() {
        Annotations.setDeleteResources(cluster, false)
        Annotations.setWithoutSavepoint(cluster, false)
        Annotations.setManualAction(cluster, ManualAction.NONE)
    }

    fun updateDigests() {
        val actualJobManagerDigest = ClusterResource.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(cluster.spec?.taskManager)
        val actualRuntimeDigest = ClusterResource.computeDigest(cluster.spec?.runtime)
        val actualBootstrapDigest = ClusterResource.computeDigest(cluster.spec?.bootstrap)
        Status.setJobManagerDigest(cluster, actualJobManagerDigest)
        Status.setTaskManagerDigest(cluster, actualTaskManagerDigest)
        Status.setRuntimeDigest(cluster, actualRuntimeDigest)
        Status.setBootstrapDigest(cluster, actualBootstrapDigest)
    }

    fun updateStatus() {
        val bootstrap = cluster.spec?.bootstrap
        Status.setBootstrap(cluster, bootstrap)
        val serviceMode = cluster.spec?.jobManager?.serviceMode
        Status.setServiceMode(cluster, serviceMode)
        val taskManagers = cluster.spec?.taskManagers ?: 0
        val taskSlots = cluster.spec?.taskManager?.taskSlots ?: 1
        Status.setTaskManagers(cluster, taskManagers)
        Status.setTaskSlots(cluster, taskSlots)
        Status.setJobParallelism(cluster, taskManagers * taskSlots)
    }

    fun computeChanges(): MutableList<String> {
        val jobManagerDigest = Status.getJobManagerDigest(cluster)
        val taskManagerDigest = Status.getTaskManagerDigest(cluster)
        val runtimeDigest = Status.getRuntimeDigest(cluster)
        val bootstrapDigest = Status.getBootstrapDigest(cluster)

        val actualJobManagerDigest = ClusterResource.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(cluster.spec?.taskManager)
        val actualRuntimeDigest = ClusterResource.computeDigest(cluster.spec?.runtime)
        val actualBootstrapDigest = ClusterResource.computeDigest(cluster.spec?.bootstrap)

        val changes = mutableListOf<String>()

        if (jobManagerDigest != actualJobManagerDigest) {
            changes.add("JOB_MANAGER")
        }

        if (taskManagerDigest != actualTaskManagerDigest) {
            changes.add("TASK_MANAGER")
        }

        if (runtimeDigest != actualRuntimeDigest) {
            changes.add("RUNTIME")
        }

        if (bootstrapDigest != actualBootstrapDigest) {
            changes.add("BOOTSTRAP")
        }

        return changes
    }

    fun setClusterStatus(status: ClusterStatus) {
        Status.setClusterStatus(cluster, status)
    }

    fun getClusterStatus(): ClusterStatus = Status.getClusterStatus(cluster)

    fun resetManualAction() {
        Annotations.setManualAction(cluster, ManualAction.NONE)
    }

    fun setDeleteResources(value: Boolean) {
        Annotations.setDeleteResources(cluster, value)
    }

    fun setTaskStatus(status: TaskStatus) {
        Status.setTaskStatus(cluster, status)
    }

    fun getTaskStatus(): TaskStatus = Status.getCurrentTaskStatus(cluster)

    fun resetSavepointRequest() {
        Status.resetSavepointRequest(cluster)
    }

    fun setSavepointRequest(request: SavepointRequest) {
        Status.setSavepointRequest(cluster, request)
    }

    fun getSavepointRequest(): SavepointRequest? = Status.getSavepointRequest(cluster)

    fun setSavepointPath(path: String) {
        Status.setSavepointPath(cluster, path)
    }

    fun isBootstrapPresent(): Boolean = Status.getBootstrap(cluster) != null

    fun getSavepointMode(): String? = Status.getSavepointMode(cluster)

    fun rescaleCluster() {
        val desiredTaskManagers = cluster.spec?.taskManagers ?: 1
        val currentTaskSlots = cluster.status?.taskSlots ?: 1
        Status.setTaskManagers(cluster, desiredTaskManagers)
        Status.setTaskSlots(cluster, currentTaskSlots)
        Status.setJobParallelism(cluster, desiredTaskManagers * currentTaskSlots)
    }

    fun getTaskManagers(): Int = Status.getTaskManagers(cluster)

    fun createBootstrapJob(clusterId: ClusterId): OperationResult<String?> {
        val savepointPath = Status.getSavepointPath(cluster)
        val parallelism = Status.getJobParallelism(cluster)

        val resource = when (Annotations.isWithoutSavepoint(cluster)) {
            true ->
                DefaultBootstrapJobFactory.createBootstrapJob(
                    clusterId, "flink-operator", cluster.status.bootstrap, null, parallelism
                )
            else ->
                DefaultBootstrapJobFactory.createBootstrapJob(
                    clusterId, "flink-operator", cluster.status.bootstrap, savepointPath, parallelism
                )
        }

        return createBootstrapJob(clusterId, resource)
    }

    fun createJobManagerService(clusterId: ClusterId): OperationResult<String?> {
        val resource = DefaultClusterResourcesFactory.createJobManagerService(
            clusterId.namespace, clusterId.uuid, "flink-operator", cluster
        )

        return createJobManagerService(clusterId, resource)
    }

    fun createJobManagerStatefulSet(clusterId: ClusterId): OperationResult<String?> {
        val resource = DefaultClusterResourcesFactory.createJobManagerStatefulSet(
            clusterId.namespace, clusterId.uuid, "flink-operator", cluster
        )

        return createStatefulSet(clusterId, resource)
    }

    fun createTaskManagerStatefulSet(clusterId: ClusterId): OperationResult<String?> {
        val resource = DefaultClusterResourcesFactory.createTaskManagerStatefulSet(
            clusterId.namespace, clusterId.uuid, "flink-operator", cluster
        )

        return createStatefulSet(clusterId, resource)
    }

    fun getClusterScale(): ClusterScaling {
        val taskManagers = Status.getTaskManagers(cluster)
        val taskSlots = Status.getTaskSlots(cluster)

        return ClusterScaling(
            taskManagers = taskManagers, taskSlots = taskSlots
        )
    }

    fun getActionTimestamp(): DateTime = Annotations.getActionTimestamp(cluster)

    fun getStatusTimestamp(): DateTime = Status.getStatusTimestamp(cluster)

    fun getJobRestartPolicy(): String? = Status.getJobRestartPolicy(cluster)

    fun isSavepointRequired(): Boolean = !Annotations.isWithoutSavepoint(cluster) && !Annotations.isDeleteResources(cluster)

    fun getSavepointOtions(): SavepointOptions {
        return SavepointOptions(
            targetPath = Configuration.getSavepointTargetPath(cluster)
        )
    }

    fun doesBootstrapExists(): Boolean = resources.bootstrapJob != null

    fun doesJobManagerServiceExists(): Boolean = resources.jobmanagerService != null

    fun doesJobManagerStatefulSetExists(): Boolean = resources.jobmanagerStatefulSet != null

    fun doesTaskManagerStatefulSetExists(): Boolean = resources.taskmanagerStatefulSet != null

    fun doesJobManagerPVCExists(): Boolean = resources.jobmanagerPVC != null

    fun doesTaskManagerPVCExists(): Boolean = resources.taskmanagerPVC != null

    fun getManualAction(): ManualAction = Annotations.getManualAction(cluster)

    fun getSavepointInterval(): Long = Configuration.getSavepointInterval(cluster)

    fun isDeleteResources(): Boolean = Annotations.isDeleteResources(cluster)

    fun getDesiredTaskManagers(): Int = cluster.spec?.taskManagers ?: 1

    fun getJobManagerReplicas(): Int = resources.jobmanagerStatefulSet?.status?.replicas ?: 0

    fun getTaskManagerReplicas(): Int = resources.taskmanagerStatefulSet?.status?.replicas ?: 0
}
