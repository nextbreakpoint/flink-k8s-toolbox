package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.RescalePolicy
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.k8s.common.FlinkClusterAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkClusterConfiguration
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.common.FlinkJobAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.factory.ClusterResourcesDefaultFactory
import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service
import org.joda.time.DateTime
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.max
import kotlin.math.min

class ClusterController(
    val namespace: String,
    val clusterName: String,
    private val controller: Controller,
    private val pollingInterval: Long,
    private val resources: ClusterResources,
    private val cluster: V1FlinkCluster
) {
    fun timeSinceLastUpdateInSeconds() = (controller.currentTimeMillis() - FlinkClusterStatus.getStatusTimestamp(cluster).millis) / 1000L

    fun timeSinceLastRescaleInSeconds() = (controller.currentTimeMillis() - FlinkClusterStatus.getRescaleTimestamp(cluster).millis) / 1000L

    fun removeJars() = controller.removeJars(namespace, clusterName)

    fun createPod(pod: V1Pod) = controller.createPod(namespace, clusterName, pod)

    fun deletePod(name: String) = controller.deletePod(namespace, clusterName, name)

    fun createService(service: V1Service) = controller.createService(namespace, clusterName, service)

    fun deleteService() = resources.jobmanagerService?.metadata?.name?.let { controller.deleteService(namespace, clusterName, it) } ?: Result(ResultStatus.OK, null)

    fun stopJobs(excludeJobIds: Set<String>) = controller.stopJobs(namespace, clusterName, excludeJobIds)

    fun deleteTaskManagers() {
        resources.taskmanagerPods.forEach { pod -> pod.metadata?.name?.let { deletePod(it) } }
    }

    fun deleteJobManagers() {
        resources.jobmanagerPods.forEach { pod -> pod.metadata?.name?.let { deletePod(it) } }
    }

    fun isClusterReady() = controller.isClusterReady(namespace, clusterName, getRequiredTaskSlots())

    fun isClusterHealthy() = controller.isClusterHealthy(namespace, clusterName)

    fun refreshStatus(logger: Logger, statusTimestamp: DateTime, actionTimestamp: DateTime, hasFinalizer: Boolean) {
        FlinkClusterStatus.setTaskManagerReplicas(cluster, getTaskManagerReplicas())

        FlinkClusterStatus.setTotalTaskSlots(cluster, getTaskManagerReplicas() * getDeclaredTaskSlots())

        FlinkClusterStatus.setTaskManagers(cluster, getClampedTaskManagers())

        val newStatusTimestamp = FlinkClusterStatus.getStatusTimestamp(cluster)

        if (statusTimestamp != newStatusTimestamp) {
            logger.log(Level.FINE, "Updating status")
            controller.updateStatus(namespace, clusterName, cluster)
        }

        val newActionTimestamp = FlinkClusterAnnotations.getActionTimestamp(cluster)

        if (actionTimestamp != newActionTimestamp) {
            logger.log(Level.FINE, "Updating annotations")
            controller.updateAnnotations(namespace, clusterName, cluster)
        }

        val newHasFinalizer = hasFinalizer()

        if (hasFinalizer != newHasFinalizer) {
            logger.log(Level.FINE, "Updating finalizers")
            controller.updateFinalizers(namespace, clusterName, cluster)
        }
    }

    fun hasBeenDeleted() = cluster.metadata.deletionTimestamp != null

    fun hasFinalizer() = cluster.metadata.finalizers.orEmpty().contains(Resource.SUPERVISOR_FINALIZER_VALUE)

    fun addFinalizer() {
        val finalizers = cluster.metadata.finalizers ?: listOf()
        if (!finalizers.contains(Resource.SUPERVISOR_FINALIZER_VALUE)) {
            cluster.metadata.finalizers = finalizers.plus(Resource.SUPERVISOR_FINALIZER_VALUE)
        }
    }

    fun removeFinalizer() {
        val finalizers = cluster.metadata.finalizers
        if (finalizers != null && finalizers.contains(Resource.SUPERVISOR_FINALIZER_VALUE)) {
            cluster.metadata.finalizers = finalizers.minus(Resource.SUPERVISOR_FINALIZER_VALUE)
        }
    }

    fun initializeStatus() {
        val labelSelector = Resource.makeLabelSelector(clusterName)
        FlinkClusterStatus.setLabelSelector(cluster, labelSelector)

        updateStatus()
    }

    fun initializeAnnotations() {
        FlinkClusterAnnotations.setDeleteResources(cluster, false)
        FlinkClusterAnnotations.setWithoutSavepoint(cluster, false)
        FlinkClusterAnnotations.setRequestedAction(cluster, Action.NONE)
    }

    fun updateDigests() {
        val jobmanagerDigest = Resource.computeDigest(cluster.spec.jobManager)
        FlinkClusterStatus.setJobManagerDigest(cluster, jobmanagerDigest)

        val taskmanagerDigest = Resource.computeDigest(cluster.spec.taskManager)
        FlinkClusterStatus.setTaskManagerDigest(cluster, taskmanagerDigest)

        val runtimeDigest = Resource.computeDigest(cluster.spec.runtime)
        FlinkClusterStatus.setRuntimeDigest(cluster, runtimeDigest)

        val supervisorDigest = Resource.computeDigest(cluster.spec.supervisor)
        FlinkClusterStatus.setSupervisorDigest(cluster, supervisorDigest)
    }

    fun updateStatus() {
        FlinkClusterStatus.setTotalTaskSlots(cluster, getTaskManagerReplicas() * getDeclaredTaskSlots())

        FlinkClusterStatus.setTaskManagers(cluster, getClampedTaskManagers())

        FlinkClusterStatus.setTaskSlots(cluster, getDeclaredTaskSlots())

        FlinkClusterStatus.setServiceMode(cluster, getDeclaredServiceMode())
    }

    fun computeChanges(): List<String> {
        val jobManagerDigest = FlinkClusterStatus.getJobManagerDigest(cluster)
        val taskManagerDigest = FlinkClusterStatus.getTaskManagerDigest(cluster)
        val runtimeDigest = FlinkClusterStatus.getRuntimeDigest(cluster)

        val actualJobManagerDigest = Resource.computeDigest(cluster.spec.jobManager)
        val actualTaskManagerDigest = Resource.computeDigest(cluster.spec.taskManager)
        val actualRuntimeDigest = Resource.computeDigest(cluster.spec.runtime)

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

        return changes
    }

    fun setSupervisorStatus(status: ClusterStatus) {
        FlinkClusterStatus.setSupervisorStatus(cluster, status)
    }

    fun getSupervisorStatus() = FlinkClusterStatus.getSupervisorStatus(cluster)

    fun setResourceStatus(status: ResourceStatus) {
        FlinkClusterStatus.setResourceStatus(cluster, status)
    }

    fun getResourceStatus() = FlinkClusterStatus.getResourceStatus(cluster)

    fun resetAction() {
        FlinkClusterAnnotations.setRequestedAction(cluster, Action.NONE)
    }

    fun getAction() = FlinkClusterAnnotations.getRequestedAction(cluster)

    fun setDeleteResources(value: Boolean) {
        FlinkClusterAnnotations.setDeleteResources(cluster, value)
    }

    fun isDeleteResources() = FlinkClusterAnnotations.isDeleteResources(cluster)

    fun setWithoutSavepoint(withoutSavepoint: Boolean) {
        FlinkClusterAnnotations.setWithoutSavepoint(cluster, withoutSavepoint)
    }

    fun isWithoutSavepoint() = FlinkClusterAnnotations.isWithoutSavepoint(cluster)

    fun setShouldRestart(value: Boolean) {
        FlinkClusterAnnotations.setShouldRestart(cluster, value)
    }

    fun shouldRestart() = FlinkClusterAnnotations.shouldRestart(cluster)

    fun setClusterHealth(health: String) {
        FlinkClusterStatus.setClusterHealth(cluster, health)
    }

    fun createService(): Result<String?> {
        val resource = ClusterResourcesDefaultFactory.createService(
            namespace,
            Resource.RESOURCE_OWNER,
            cluster.metadata?.name ?: throw RuntimeException("Metadata name is null"),
            cluster.spec
        )

        return createService(resource)
    }

    fun createJobManagerPods(replicas: Int): Result<Set<String>> {
        if (resources.jobmanagerPods.size == replicas) {
            return Result(ResultStatus.OK, setOf())
        }

        val resource = ClusterResourcesDefaultFactory.createJobManagerPod(
            namespace,
            Resource.RESOURCE_OWNER,
            cluster.metadata?.name ?: throw RuntimeException("Metadata name is null"),
            cluster.spec
        )

        return if (resources.jobmanagerPods.size > replicas) {
            Result(ResultStatus.OK, setOf())
        } else {
            val sequence = (1 .. (replicas - resources.jobmanagerPods.size)).asSequence()

            val results = sequence.map { createPod(resource) }.map { it.output }.filterNotNull().toSet()

            Result(ResultStatus.OK, results)
        }
    }

    fun createTaskManagerPods(replicas: Int): Result<Set<String>> {
        if (resources.taskmanagerPods.size == replicas) {
            return Result(ResultStatus.OK, setOf())
        }

        val resource = ClusterResourcesDefaultFactory.createTaskManagerPod(
            namespace,
            Resource.RESOURCE_OWNER,
            cluster.metadata?.name ?: throw RuntimeException("Metadata name is null"),
            cluster.spec
        )

        return if (resources.taskmanagerPods.size > replicas) {
            Result(ResultStatus.OK, setOf())
        } else {
            val sequence = (1 .. (replicas - resources.taskmanagerPods.size)).asSequence()

            val results = sequence.map { createPod(resource) }.map { it.output }.filterNotNull().toSet()

            Result(ResultStatus.OK, results)
        }
    }

    fun getActionTimestamp() = FlinkClusterAnnotations.getActionTimestamp(cluster)

    fun getStatusTimestamp() = FlinkClusterStatus.getStatusTimestamp(cluster)

    fun doesJobManagerServiceExists() = resources.jobmanagerService != null

    fun doesJobManagerPodExists() = resources.jobmanagerPods.isNotEmpty()

    fun doesTaskManagerPodsExist() = resources.taskmanagerPods.isNotEmpty()

    fun getJobNamesWithStatus() = resources.flinkJobs.map {
        (it.metadata?.name ?: throw RuntimeException("Metadata name is null")) to (it.status?.supervisorStatus ?: JobStatus.Unknown)
    }.toMap()

    fun getJobNamesWithIds() = resources.flinkJobs.map {
        (it.metadata?.name ?: throw RuntimeException("Metadata name is null")) to it.status?.jobId
    }.toMap()

    fun getClampedTaskManagers() = min(max(getDeclaredTaskManagers(), cluster.spec.minTaskManagers ?: 0), cluster.spec.maxTaskManagers ?: 32)

    fun getDeclaredTaskManagers() = cluster.spec.taskManagers ?: 0

    fun getDeclaredTaskSlots() = cluster.spec.taskManager?.taskSlots ?: 1

    fun getDeclaredServiceMode() = cluster.spec.jobManager?.serviceMode

    fun getCurrentTaskManagers() = FlinkClusterStatus.getTaskManagers(cluster)

    fun getClampedRequiredTaskManagers(): Int {
        if (getRescalePolicy() == RescalePolicy.None || resources.flinkJobs.isNullOrEmpty()) {
            return getClampedTaskManagers()
        } else {
            return getRequiredTaskManagers()
        }
    }

    fun getRequiredTaskManagers(): Int {
        return min(max(computeRequiredTaskManagers(), cluster.spec.minTaskManagers ?: 0), cluster.spec.maxTaskManagers ?: 32)
    }

    fun getRequiredTaskSlots() = resources.flinkJobs
        .filter { job -> activeStatus.contains(job.status?.supervisorStatus) }
        .map { job -> job.status?.jobParallelism ?: 0 }.sum()

    fun getJobManagerReplicas() = resources.jobmanagerPods.size

    fun getTaskManagerReplicas() = resources.taskmanagerPods.size

    fun areJobsUpdating() = resources.flinkJobs.any { job -> !isJobReady(job) }

    fun getRescaleDelay() = FlinkClusterConfiguration.getRescaleDelay(cluster)

    fun getRescalePolicy() = FlinkClusterConfiguration.getRescalePolicy(cluster)

    fun rescaleCluster(requiredTaskManagers: Int) {
        FlinkClusterStatus.setTaskManagers(cluster, requiredTaskManagers)

        controller.updateTaskManagerReplicas(namespace, clusterName, requiredTaskManagers)
    }

    fun removeUnusedTaskManagers(): Map<String, String?> {
        val taskManagersStatusResult = controller.getTaskManagerStatus(namespace, clusterName)

        if (!taskManagersStatusResult.isSuccessful()) {
            return mapOf()
        }

        val pods: Map<String, String> = taskManagersStatusResult.output?.taskmanagers?.map {
            it.id to if (it.slotsNumber == it.freeSlots) getPodName(it).orEmpty() else ""
        }?.filter { it.second.isNotEmpty() }?.toMap().orEmpty()

        pods.forEach { controller.deletePod(namespace, clusterName, it.value) }

        return pods
    }

    fun updateRescaleTimestamp() {
        FlinkClusterStatus.updateRescaleTimestamp(cluster, controller.currentTimeMillis())
    }

    fun hasJobFinalizers() = resources.flinkJobs.any {
        it.metadata?.finalizers?.contains(Resource.SUPERVISOR_FINALIZER_VALUE) ?: false
    }

    private fun computeRequiredTaskManagers(): Int {
        val requiredTaskSlots = getRequiredTaskSlots()
        val taskSlots = getDeclaredTaskSlots()
        return (requiredTaskSlots + taskSlots / 2) / taskSlots
    }

    private fun getPodName(taskManagerInfo: TaskManagerInfo): String? {
        //akka.tcp://flink@172.17.0.12:41545/user/taskmanager_0
        val regexp = Regex("akka\\.tcp://flink@([0-9.]+):[0-9]+/user/taskmanager_[0-9]+")
        val match = regexp.matchEntire(taskManagerInfo.path.toString())
        val nodeIP = match?.groupValues?.get(1)
        return if (nodeIP != null) findTaskManagerByPodIP(nodeIP) else null
    }

    private fun findTaskManagerByPodIP(podIP: String?) =
        resources.taskmanagerPods.find { pod -> pod.status?.podIP == podIP }?.metadata?.name

    private fun hasJobStatusChangedRecently(job: V1FlinkJob) =
        (controller.currentTimeMillis() - FlinkJobStatus.getStatusTimestamp(job).millis) / 1000L < pollingInterval * 2

    private fun isJobReady(job: V1FlinkJob) = job.status?.resourceStatus == ResourceStatus.Updated.toString() && !transitoryStatus.contains(job.status?.supervisorStatus.toString())

    private val activeStatus = setOf(JobStatus.Starting.toString(), JobStatus.Started.toString())

    private val transitoryStatus = setOf(JobStatus.Starting.toString(), JobStatus.Stopping.toString())
}