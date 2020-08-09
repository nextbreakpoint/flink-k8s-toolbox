package com.nextbreakpoint.flinkoperator.server.supervisor.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.ClusterScale
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.DeleteOptions
import com.nextbreakpoint.flinkoperator.common.ManualAction
import com.nextbreakpoint.flinkoperator.common.PodReplicas
import com.nextbreakpoint.flinkoperator.common.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.SavepointRequest
import com.nextbreakpoint.flinkoperator.server.common.Resource
import com.nextbreakpoint.flinkoperator.server.common.Annotations
import com.nextbreakpoint.flinkoperator.server.common.Configuration
import com.nextbreakpoint.flinkoperator.server.controller.Controller
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import com.nextbreakpoint.flinkoperator.server.common.Status
import com.nextbreakpoint.flinkoperator.server.factory.BootstrapResourcesDefaultFactory
import com.nextbreakpoint.flinkoperator.server.factory.ClusterResourcesDefaultFactory
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service
import org.apache.log4j.Logger
import org.joda.time.DateTime

class TaskController(
    val clusterSelector: ClusterSelector,
    private val cluster: V1FlinkCluster,
    private val resources: CachedResources,
    private val controller: Controller
) {
    fun timeSinceLastUpdateInSeconds() = (controller.currentTimeMillis() - Status.getStatusTimestamp(cluster).millis) / 1000L

    fun timeSinceLastSavepointRequestInSeconds() = (controller.currentTimeMillis() - Status.getSavepointRequestTimestamp(cluster).millis) / 1000L

    fun removeJar(clusterSelector: ClusterSelector) : Result<Void?> =
        controller.removeJar(clusterSelector)

    fun triggerSavepoint(clusterSelector: ClusterSelector, options: SavepointOptions) : Result<SavepointRequest?> =
        controller.triggerSavepoint(clusterSelector, options)

    fun querySavepoint(clusterSelector: ClusterSelector, savepointRequest: SavepointRequest) : Result<String?> =
        controller.querySavepoint(clusterSelector, savepointRequest)

    fun createBootstrapJob(clusterSelector: ClusterSelector, bootstrapJob: V1Job): Result<String?> =
        controller.createBootstrapJob(clusterSelector, bootstrapJob)

    fun deleteBootstrapJob(clusterSelector: ClusterSelector) : Result<Void?> =
        controller.deleteBootstrapJob(clusterSelector)

    fun createPods(clusterSelector: ClusterSelector, options: PodReplicas): Result<Set<String>> =
        controller.createPods(clusterSelector, options)

    fun deletePods(clusterSelector: ClusterSelector, options: DeleteOptions) : Result<Void?> =
        controller.deletePods(clusterSelector, options)

    fun arePodsRunning(clusterSelector: ClusterSelector): Result<Boolean> =
        controller.arePodsRunning(clusterSelector)

    fun arePodsTerminated(clusterSelector: ClusterSelector): Result<Boolean> =
        controller.arePodsTerminated(clusterSelector)

    fun startJob(clusterSelector: ClusterSelector, cluster: V1FlinkCluster) : Result<Void?> =
        controller.startJob(clusterSelector, cluster)

    fun stopJob(clusterSelector: ClusterSelector): Result<Boolean> =
        controller.stopJob(clusterSelector)

    fun cancelJob(clusterSelector: ClusterSelector, options: SavepointOptions): Result<SavepointRequest?> =
        controller.cancelJob(clusterSelector, options)

    fun isClusterReady(clusterSelector: ClusterSelector, options: ClusterScale): Result<Boolean> =
        controller.isClusterReady(clusterSelector, options)

    fun isJobFinished(clusterSelector: ClusterSelector): Result<Boolean> =
        controller.isJobFinished(clusterSelector)

    fun isJobRunning(clusterSelector: ClusterSelector): Result<Boolean> =
        controller.isJobRunning(clusterSelector)

    fun isJobFailed(clusterSelector: ClusterSelector): Result<Boolean> =
        controller.isJobFailed(clusterSelector)

    fun isJobCancelled(clusterSelector: ClusterSelector): Result<Boolean> =
        controller.isJobCancelled(clusterSelector)

    fun createService(clusterSelector: ClusterSelector, service: V1Service): Result<String?> =
        controller.createService(clusterSelector, service)

    fun deleteService(clusterSelector: ClusterSelector): Result<Void?> =
        controller.deleteService(clusterSelector)

    fun refreshStatus(logger: Logger, statusTimestamp: DateTime, actionTimestamp: DateTime, hasFinalizer: Boolean) {
        val taskManagers = resources.taskmanagerPods.size
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

        val restartPolicy = cluster.spec?.operator?.restartPolicy
        if (Status.getRestartPolicy(cluster) != restartPolicy) {
            Status.setRestartPolicy(cluster, restartPolicy)
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
        if (cluster.metadata.finalizers != null && cluster.metadata.finalizers.contains("finalizer.nextbreakpoint.com")) {
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
        Status.setSavepointPath(
            cluster, savepointPath ?: ""
        )

        val labelSelector = Resource.makeLabelSelector(clusterSelector)
        Status.setLabelSelector(cluster, labelSelector)

        val serviceMode = cluster.spec?.jobManager?.serviceMode
        Status.setServiceMode(cluster, serviceMode)

        val savepointMode = cluster.spec?.operator?.savepointMode
        Status.setSavepointMode(cluster, savepointMode)

        val restartPolicy = cluster.spec?.operator?.restartPolicy
        Status.setRestartPolicy(cluster, restartPolicy)
    }

    fun initializeAnnotations() {
        Annotations.setDeleteResources(cluster, false)
        Annotations.setWithoutSavepoint(cluster, false)
        Annotations.setManualAction(cluster, ManualAction.NONE)
    }

    fun updateDigests() {
        val actualJobManagerDigest = Resource.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = Resource.computeDigest(cluster.spec?.taskManager)
        val actualRuntimeDigest = Resource.computeDigest(cluster.spec?.runtime)
        val actualBootstrapDigest = Resource.computeDigest(cluster.spec?.bootstrap)
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

        val actualJobManagerDigest = Resource.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = Resource.computeDigest(cluster.spec?.taskManager)
        val actualRuntimeDigest = Resource.computeDigest(cluster.spec?.runtime)
        val actualBootstrapDigest = Resource.computeDigest(cluster.spec?.bootstrap)

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
        // delete savepoint request when changing state
        Status.resetSavepointRequest(cluster)
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

    fun createBootstrapJob(clusterSelector: ClusterSelector): Result<String?> {
        val savepointPath = Status.getSavepointPath(cluster)
        val parallelism = Status.getJobParallelism(cluster)

        val resource = when (Annotations.isWithoutSavepoint(cluster)) {
            true ->
                BootstrapResourcesDefaultFactory.createBootstrapJob(
                    clusterSelector, "flink-operator", cluster.status.bootstrap, null, parallelism
                )
            else ->
                BootstrapResourcesDefaultFactory.createBootstrapJob(
                    clusterSelector, "flink-operator", cluster.status.bootstrap, savepointPath, parallelism
                )
        }

        return createBootstrapJob(clusterSelector, resource)
    }

    fun createService(clusterSelector: ClusterSelector): Result<String?> {
        val resource = ClusterResourcesDefaultFactory.createService(
            clusterSelector.namespace, clusterSelector.uuid, "flink-operator", cluster
        )

        return createService(clusterSelector, resource)
    }

    fun createJobManagerPods(clusterSelector: ClusterSelector, replicas: Int): Result<Set<String>> {
        if (resources.jobmanagerPods.size == replicas) {
            return Result(ResultStatus.OK, setOf())
        }

        val resource = ClusterResourcesDefaultFactory.createJobManagerPod(
            clusterSelector.namespace, clusterSelector.uuid, "flink-operator", cluster
        )

        return if (resources.jobmanagerPods.size > replicas) {
            deletePods(clusterSelector, DeleteOptions(label = "role", value = "jobmanager", limit = resources.jobmanagerPods.size - replicas))
            Result(ResultStatus.OK, setOf())
        } else {
            createPods(clusterSelector, PodReplicas(resource, replicas - resources.jobmanagerPods.size))
            Result(ResultStatus.OK, setOf())
        }
    }

    fun createTaskManagerPods(clusterSelector: ClusterSelector, replicas: Int): Result<Set<String>> {
        if (resources.taskmanagerPods.size == replicas) {
            return Result(ResultStatus.OK, setOf())
        }

        val resource = ClusterResourcesDefaultFactory.createTaskManagerPod(
            clusterSelector.namespace, clusterSelector.uuid, "flink-operator", cluster
        )

        return if (resources.taskmanagerPods.size > replicas) {
            deletePods(clusterSelector, DeleteOptions(label = "role", value = "taskmanager", limit = resources.taskmanagerPods.size - replicas))
            Result(ResultStatus.OK, setOf())
        } else {
            createPods(clusterSelector, PodReplicas(resource, replicas - resources.taskmanagerPods.size))
            Result(ResultStatus.OK, setOf())
        }
    }

    fun getClusterScale() =
        ClusterScale(
            taskManagers = Status.getTaskManagers(cluster), taskSlots = Status.getTaskSlots(cluster)
        )

    fun getActionTimestamp(): DateTime = Annotations.getActionTimestamp(cluster)

    fun getStatusTimestamp(): DateTime = Status.getStatusTimestamp(cluster)

    fun getRestartPolicy(): String? = Status.getRestartPolicy(cluster)

    fun isSavepointRequired(): Boolean =
        !Annotations.isWithoutSavepoint(cluster) && !Annotations.isDeleteResources(cluster) && timeSinceLastSavepointRequestInSeconds() >= getSavepointInterval()

    fun getSavepointOptions() =
        SavepointOptions(
            targetPath = Configuration.getSavepointTargetPath(cluster)
        )

    fun doesBootstrapJobExists(): Boolean = resources.bootstrapJob != null

    fun doesServiceExists(): Boolean = resources.service != null

    fun doesJobManagerPodsExists(): Boolean = resources.jobmanagerPods.isNotEmpty()

    fun doesTaskManagerPodsExists(): Boolean = resources.taskmanagerPods.isNotEmpty()

    fun getManualAction(): ManualAction = Annotations.getManualAction(cluster)

    fun getSavepointInterval(): Long = Configuration.getSavepointInterval(cluster)

    fun isDeleteResources(): Boolean = Annotations.isDeleteResources(cluster)

    fun getDesiredTaskManagers(): Int = cluster.spec?.taskManagers ?: 1

    fun getJobManagerReplicas(): Int = resources.jobmanagerPods.size

    fun getTaskManagerReplicas(): Int = resources.taskmanagerPods.size
}
