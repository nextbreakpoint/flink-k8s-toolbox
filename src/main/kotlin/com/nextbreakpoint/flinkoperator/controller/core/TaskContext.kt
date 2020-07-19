package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultBootstrapJobFactory
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import org.apache.log4j.Logger
import org.joda.time.DateTime

class TaskContext(
    val clusterSelector: ClusterSelector,
    private val cluster: V1FlinkCluster,
    private val resources: CachedResources,
    private val controller: OperationController
) {
    fun timeSinceLastUpdateInSeconds() = (controller.currentTimeMillis() - Status.getStatusTimestamp(cluster).millis) / 1000L

    fun timeSinceLastSavepointRequestInSeconds() = (controller.currentTimeMillis() - Status.getSavepointRequestTimestamp(cluster).millis) / 1000L

    fun removeJar(clusterSelector: ClusterSelector) : OperationResult<Void?> =
        controller.removeJar(clusterSelector)

    fun triggerSavepoint(clusterSelector: ClusterSelector, options: SavepointOptions) : OperationResult<SavepointRequest> =
        controller.triggerSavepoint(clusterSelector, options)

    fun getLatestSavepoint(clusterSelector: ClusterSelector, savepointRequest: SavepointRequest) : OperationResult<String> =
        controller.getLatestSavepoint(clusterSelector, savepointRequest)

    fun createBootstrapJob(clusterSelector: ClusterSelector, bootstrapJob: V1Job): OperationResult<String?> =
        controller.createBootstrapJob(clusterSelector, bootstrapJob)

    fun deleteBootstrapJob(clusterSelector: ClusterSelector) : OperationResult<Void?> =
        controller.deleteBootstrapJob(clusterSelector)

    fun terminatePods(clusterSelector: ClusterSelector) : OperationResult<Void?> =
        controller.terminatePods(clusterSelector)

    fun restartPods(clusterSelector: ClusterSelector, options: ClusterScaling): OperationResult<Void?> =
        controller.restartPods(clusterSelector, options)

    fun arePodsTerminated(clusterSelector: ClusterSelector): OperationResult<Boolean> =
        controller.arePodsTerminated(clusterSelector)

    fun startJob(clusterSelector: ClusterSelector, cluster: V1FlinkCluster) : OperationResult<Void?> =
        controller.startJob(clusterSelector, cluster)

    fun stopJob(clusterSelector: ClusterSelector): OperationResult<Void?> =
        controller.stopJob(clusterSelector)

    fun cancelJob(clusterSelector: ClusterSelector, options: SavepointOptions): OperationResult<SavepointRequest?> =
        controller.cancelJob(clusterSelector, options)

    fun isClusterReady(clusterSelector: ClusterSelector, options: ClusterScaling): OperationResult<Boolean> =
        controller.isClusterReady(clusterSelector, options)

    fun isJobFinished(clusterSelector: ClusterSelector): OperationResult<Boolean> =
        controller.isJobFinished(clusterSelector)

    fun isJobRunning(clusterSelector: ClusterSelector): OperationResult<Boolean> =
        controller.isJobRunning(clusterSelector)

    fun isJobFailed(clusterSelector: ClusterSelector): OperationResult<Boolean> =
        controller.isJobFailed(clusterSelector)

    fun createJobManagerService(clusterSelector: ClusterSelector, service: V1Service): OperationResult<String?> =
        controller.createJobManagerService(clusterSelector, service)

    fun deleteJobManagerService(clusterSelector: ClusterSelector): OperationResult<Void?> =
        controller.deleteJobManagerService(clusterSelector)

    fun createStatefulSet(clusterSelector: ClusterSelector, statefulSet: V1StatefulSet): OperationResult<String?> =
        controller.createStatefulSet(clusterSelector, statefulSet)

    fun deleteStatefulSets(clusterSelector: ClusterSelector): OperationResult<Void?> =
        controller.deleteStatefulSets(clusterSelector)

    fun deletePersistentVolumeClaims(clusterSelector: ClusterSelector): OperationResult<Void?> =
        controller.deletePersistentVolumeClaims(clusterSelector)

    fun refreshStatus(logger: Logger, statusTimestamp: DateTime, actionTimestamp: DateTime, hasFinalizer: Boolean) {
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
            logger.debug("Updating status")
            controller.updateStatus(clusterSelector, cluster)
        }

        val newActionTimestamp = Annotations.getActionTimestamp(cluster)

        if (actionTimestamp != newActionTimestamp) {
            logger.debug("Updating annotations")
            controller.updateAnnotations(clusterSelector, cluster)
        }

        val newHasFinalizer = hasFinalizer()

        if (hasFinalizer != newHasFinalizer) {
            logger.debug("Updating finalizers")
            controller.updateFinalizers(clusterSelector, cluster)
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

        val labelSelector = ClusterResource.makeLabelSelector(clusterSelector)
        Status.setLabelSelector(cluster, labelSelector)

        val serviceMode = cluster.spec?.jobManager?.serviceMode
        Status.setServiceMode(cluster, serviceMode)

        val savepointMode = cluster.spec?.operator?.savepointMode
        Status.setSavepointMode(cluster, savepointMode)

        val jobRestartPolicy = cluster.spec?.operator?.jobRestartPolicy
        Status.setJobRestartPolicy(cluster, jobRestartPolicy)
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

    fun computeChanges(): List<String> {
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

    fun createBootstrapJob(clusterSelector: ClusterSelector): OperationResult<String?> {
        val savepointPath = Status.getSavepointPath(cluster)
        val parallelism = Status.getJobParallelism(cluster)

        val resource = when (Annotations.isWithoutSavepoint(cluster)) {
            true ->
                DefaultBootstrapJobFactory.createBootstrapJob(
                    clusterSelector, "flink-operator", cluster.status.bootstrap, null, parallelism
                )
            else ->
                DefaultBootstrapJobFactory.createBootstrapJob(
                    clusterSelector, "flink-operator", cluster.status.bootstrap, savepointPath, parallelism
                )
        }

        return createBootstrapJob(clusterSelector, resource)
    }

    fun createJobManagerService(clusterSelector: ClusterSelector): OperationResult<String?> {
        val resource = DefaultClusterResourcesFactory.createJobManagerService(
            clusterSelector.namespace, clusterSelector.uuid, "flink-operator", cluster
        )

        return createJobManagerService(clusterSelector, resource)
    }

    fun createJobManagerStatefulSet(clusterSelector: ClusterSelector): OperationResult<String?> {
        val resource = DefaultClusterResourcesFactory.createJobManagerStatefulSet(
            clusterSelector.namespace, clusterSelector.uuid, "flink-operator", cluster
        )

        return createStatefulSet(clusterSelector, resource)
    }

    fun createTaskManagerStatefulSet(clusterSelector: ClusterSelector): OperationResult<String?> {
        val resource = DefaultClusterResourcesFactory.createTaskManagerStatefulSet(
            clusterSelector.namespace, clusterSelector.uuid, "flink-operator", cluster
        )

        return createStatefulSet(clusterSelector, resource)
    }

    fun getClusterScale() =
        ClusterScaling(
            taskManagers = Status.getTaskManagers(cluster), taskSlots = Status.getTaskSlots(cluster)
        )

    fun getActionTimestamp(): DateTime = Annotations.getActionTimestamp(cluster)

    fun getStatusTimestamp(): DateTime = Status.getStatusTimestamp(cluster)

    fun getJobRestartPolicy(): String? = Status.getJobRestartPolicy(cluster)

    fun isSavepointRequired(): Boolean = !Annotations.isWithoutSavepoint(cluster) && !Annotations.isDeleteResources(cluster)

    fun getSavepointOtions() =
        SavepointOptions(
            targetPath = Configuration.getSavepointTargetPath(cluster)
        )

    fun doesBootstrapJobExists(): Boolean = resources.bootstrapJob != null

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
