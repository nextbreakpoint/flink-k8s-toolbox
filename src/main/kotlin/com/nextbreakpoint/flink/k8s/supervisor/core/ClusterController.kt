package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.DeleteOptions
import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.common.PodReplicas
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.ResourceStatus.*
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterJobSpec
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.common.FlinkClusterAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkClusterConfiguration
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.factory.ClusterResourcesDefaultFactory
import com.nextbreakpoint.flink.k8s.factory.JobResourcesDefaultFactory
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service
import org.apache.log4j.Logger
import org.joda.time.DateTime

class ClusterController(
    val clusterSelector: ResourceSelector,
    private val cluster: V2FlinkCluster,
    private val resources: ClusterResources,
    private val controller: Controller
) {
    fun timeSinceLastUpdateInSeconds() = (controller.currentTimeMillis() - FlinkClusterStatus.getStatusTimestamp(cluster).millis) / 1000L

    fun timeSinceLastRescaleInSeconds() = (controller.currentTimeMillis() - FlinkClusterStatus.getRescaleTimestamp(cluster).millis) / 1000L

    fun removeJars() = controller.removeJars(clusterSelector)

    fun createJob(job: V1FlinkJob) = controller.createFlinkJob(clusterSelector, job)

    fun deleteJob(jobName: String) = controller.deleteFlinkJob(clusterSelector, jobName)

    fun createPods(replicas: PodReplicas) = controller.createPods(clusterSelector, replicas)

    fun deletePods(options: DeleteOptions) = controller.deletePods(clusterSelector, options)

    fun createPod(pod: V1Pod) = controller.createPod(clusterSelector, pod)

    fun deletePod(name: String) = controller.deletePod(clusterSelector, name)

    fun createService(service: V1Service) = controller.createService(clusterSelector, service)

    fun deleteService() = controller.deleteService(clusterSelector)

    fun stopJobs(excludeJobIds: Set<String>) = controller.stopJobs(clusterSelector, excludeJobIds)

    fun isClusterReady() = controller.isClusterReady(clusterSelector, getRequiredTaskSlots())

    fun isClusterHealthy() = controller.isClusterHealthy(clusterSelector)

    fun refreshStatus(logger: Logger, statusTimestamp: DateTime, actionTimestamp: DateTime, hasFinalizer: Boolean) {
        val taskManagerReplicas = getTaskManagerReplicas()
        if (FlinkClusterStatus.getActiveTaskManagers(cluster) != taskManagerReplicas) {
            FlinkClusterStatus.setActiveTaskManagers(cluster, taskManagerReplicas)
        }

        val taskSlots = getTaskSlots()
        if (FlinkClusterStatus.getTotalTaskSlots(cluster) != taskManagerReplicas * taskSlots) {
            FlinkClusterStatus.setTotalTaskSlots(cluster, taskManagerReplicas * taskSlots)
        }

        val taskManagers = getTaskManagers()
        if (FlinkClusterStatus.getTaskManagers(cluster) != taskManagers) {
            FlinkClusterStatus.setTaskManagers(cluster, taskManagers)
        }

        val newStatusTimestamp = FlinkClusterStatus.getStatusTimestamp(cluster)

        if (statusTimestamp != newStatusTimestamp) {
            logger.debug("Updating status")
            controller.updateStatus(clusterSelector, cluster)
        }

        val newActionTimestamp = FlinkClusterAnnotations.getActionTimestamp(cluster)

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

    fun hasBeenDeleted() = cluster.metadata.deletionTimestamp != null

    fun hasFinalizer() = cluster.metadata.finalizers.orEmpty().contains("finalizer.nextbreakpoint.com")

    fun addFinalizer() {
        val finalizers = cluster.metadata.finalizers ?: listOf()
        if (!finalizers.contains("finalizer.nextbreakpoint.com")) {
            cluster.metadata.finalizers = finalizers.plus("finalizer.nextbreakpoint.com")
        }
    }

    fun removeFinalizer() {
        val finalizers = cluster.metadata.finalizers
        if (finalizers != null && finalizers.contains("finalizer.nextbreakpoint.com")) {
            cluster.metadata.finalizers = finalizers.minus("finalizer.nextbreakpoint.com")
        }
    }

    fun initializeStatus() {
        val labelSelector = Resource.makeLabelSelector(clusterSelector)
        FlinkClusterStatus.setLabelSelector(cluster, labelSelector)
        updateStatus()
    }

    fun initializeAnnotations() {
        FlinkClusterAnnotations.setDeleteResources(cluster, false)
        FlinkClusterAnnotations.setWithoutSavepoint(cluster, false)
        FlinkClusterAnnotations.setManualAction(cluster, ManualAction.NONE)
    }

    fun updateDigests() {
        val jobmanagerDigest = Resource.computeDigest(cluster.spec?.jobManager)
        FlinkClusterStatus.setJobManagerDigest(cluster, jobmanagerDigest)

        val taskmanagerDigest = Resource.computeDigest(cluster.spec?.taskManager)
        FlinkClusterStatus.setTaskManagerDigest(cluster, taskmanagerDigest)

        val runtimeDigest = Resource.computeDigest(cluster.spec?.runtime)
        FlinkClusterStatus.setRuntimeDigest(cluster, runtimeDigest)

        val jobDigests = cluster.spec?.jobs?.map { it.name to Resource.computeDigest(it.spec) }.orEmpty()
        FlinkClusterStatus.setJobDigests(cluster, jobDigests)
    }

    fun updateStatus() {
        val serviceMode = cluster.spec?.jobManager?.serviceMode
        FlinkClusterStatus.setServiceMode(cluster, serviceMode)

        val taskManagers = cluster.spec?.taskManagers ?: 0
        FlinkClusterStatus.setTaskManagers(cluster, taskManagers)

        val taskSlots = cluster.spec?.taskManager?.taskSlots ?: 1
        FlinkClusterStatus.setTaskSlots(cluster, taskSlots)
    }

    fun computeChanges(): List<String> {
        val jobManagerDigest = FlinkClusterStatus.getJobManagerDigest(cluster)
        val taskManagerDigest = FlinkClusterStatus.getTaskManagerDigest(cluster)
        val runtimeDigest = FlinkClusterStatus.getRuntimeDigest(cluster)

        val actualJobManagerDigest = Resource.computeDigest(cluster.spec?.jobManager)
        val actualTaskManagerDigest = Resource.computeDigest(cluster.spec?.taskManager)
        val actualRuntimeDigest = Resource.computeDigest(cluster.spec?.runtime)

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
        FlinkClusterAnnotations.setManualAction(cluster, ManualAction.NONE)
    }

    fun getAction() = FlinkClusterAnnotations.getManualAction(cluster)

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

    fun createJob(jobSpec: V2FlinkClusterJobSpec): Result<Void?> {
        val resource = JobResourcesDefaultFactory.createJob(
            clusterSelector, "flink-operator", jobSpec
        )

        return createJob(resource)
    }

    fun createService(): Result<String?> {
        val resource = ClusterResourcesDefaultFactory.createService(
            clusterSelector.namespace, clusterSelector.uid, "flink-operator", cluster
        )

        return createService(resource)
    }

    fun createJobManagerPods(replicas: Int): Result<Set<String>> {
        if (resources.jobmanagerPods.size == replicas) {
            return Result(ResultStatus.OK, setOf())
        }

        val resource = ClusterResourcesDefaultFactory.createJobManagerPod(
            clusterSelector.namespace, clusterSelector.uid, "flink-operator", cluster
        )

        return if (resources.jobmanagerPods.size > replicas) {
            deletePods(DeleteOptions(label = "role", value = "jobmanager", limit = resources.jobmanagerPods.size - replicas))
            Result(ResultStatus.OK, setOf())
        } else {
            createPods(PodReplicas(resource, replicas - resources.jobmanagerPods.size))
            Result(ResultStatus.OK, setOf())
        }
    }

    fun createTaskManagerPods(replicas: Int): Result<Set<String>> {
        if (resources.taskmanagerPods.size == replicas) {
            return Result(ResultStatus.OK, setOf())
        }

        val resource = ClusterResourcesDefaultFactory.createTaskManagerPod(
            clusterSelector.namespace, clusterSelector.uid, "flink-operator", cluster
        )

        return if (resources.taskmanagerPods.size > replicas) {
            deletePods(DeleteOptions(label = "role", value = "taskmanager", limit = resources.taskmanagerPods.size - replicas))
            Result(ResultStatus.OK, setOf())
        } else {
            createPods(PodReplicas(resource, replicas - resources.taskmanagerPods.size))
            Result(ResultStatus.OK, setOf())
        }
    }

    fun getActionTimestamp() = FlinkClusterAnnotations.getActionTimestamp(cluster)

    fun getStatusTimestamp() = FlinkClusterStatus.getStatusTimestamp(cluster)

    fun doesJobManagerServiceExists() = resources.service != null

    fun doesJobManagerPodExists() = resources.jobmanagerPods.isNotEmpty()

    fun doesTaskManagerPodsExist() = resources.taskmanagerPods.isNotEmpty()

    fun haveJobsBeenRemoved() = resources.flinkJobs.keys.isEmpty()

    fun haveJobsBeenCreated() = resources.flinkJobs.keys.toSet() == cluster.spec?.jobs?.map { it.name }.orEmpty().toSet()

    fun listExistingJobNames() = resources.flinkJobs.keys.toList()

    fun getJobNamesWithStatus() = resources.flinkJobs.map { it.key to it.value.status.supervisorStatus }.toMap()

    fun getTaskManagers() = cluster.spec?.taskManagers ?: 0

    fun getTaskSlots() = cluster.spec?.taskManager?.taskSlots ?: 1

    fun getCurrentTaskManagers() = FlinkClusterStatus.getTaskManagers(cluster)

    fun getRequiredTaskManagers(): Int {
        if (cluster.spec.jobs.isNullOrEmpty()) {
            return cluster.spec.taskManagers
        } else {
            val requiredTaskSlots = getRequiredTaskSlots()
            val taskSlots = cluster.spec.taskManager.taskSlots ?: 1
            return (requiredTaskSlots + taskSlots / 2) / taskSlots
        }
    }

    fun getRequiredTaskSlots() = resources.flinkJobs.values.map { job -> job.status.jobParallelism ?: 0 }.sum()

    fun getJobManagerReplicas() = resources.jobmanagerPods.size

    fun getTaskManagerReplicas() = resources.taskmanagerPods.size

    fun getJobSpecs() = cluster.spec?.jobs.orEmpty()

    fun getJobIds() = resources.flinkJobs.map { it.value.status.jobId }.toSet()

    fun areJobsUpdating() = resources.flinkJobs.values.any{ job -> job.status.resourceStatus == Updating.toString() }

    fun getRescaleDelay() = FlinkClusterConfiguration.getRescaleDelay(cluster)

    fun rescaleCluster(requiredTaskManagers: Int) {
        FlinkClusterStatus.setTaskManagers(cluster, requiredTaskManagers)
        controller.updateTaskManagerReplicas(clusterSelector, requiredTaskManagers)
    }

    fun removeUnusedTaskManagers(): Map<String, String?> {
        val taskManagersStatusResult = controller.getTaskManagerStatus(clusterSelector)

        if (!taskManagersStatusResult.isSuccessful()) {
            return mapOf()
        }

        val pods: Map<String, String> = taskManagersStatusResult.output?.taskmanagers?.map {
            it.id to if (it.slotsNumber == it.freeSlots) getPodName(it).orEmpty() else ""
        }?.filter { it.second.isNotEmpty() }?.toMap().orEmpty()

        pods.forEach { controller.deletePod(clusterSelector, it.value) }

        return pods
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
}