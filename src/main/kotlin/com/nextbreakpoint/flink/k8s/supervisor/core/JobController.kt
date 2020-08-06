package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.common.SavepointOptions
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.k8s.common.FlinkClusterAnnotations
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.common.FlinkJobConfiguration
import com.nextbreakpoint.flink.k8s.common.FlinkJobAnnotations
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.factory.BootstrapResourcesDefaultFactory
import io.kubernetes.client.openapi.models.V1Job
import org.apache.log4j.Logger
import org.joda.time.DateTime

class JobController(
    val clusterSelector: ResourceSelector,
    val jobSelector: ResourceSelector,
    private val cluster: V2FlinkCluster,
    private val job: V1FlinkJob,
    private val clusterResources: ClusterResources,
    private val jobResources: JobResources,
    private val controller: Controller
) {
    fun timeSinceLastUpdateInSeconds() = (controller.currentTimeMillis() - FlinkJobStatus.getStatusTimestamp(job).millis) / 1000L

    fun timeSinceLastSavepointRequestInSeconds() = (controller.currentTimeMillis() - FlinkJobStatus.getSavepointRequestTimestamp(job).millis) / 1000L

    fun triggerSavepoint(options: SavepointOptions) = controller.triggerSavepoint(clusterSelector, options, JobContext(job))

    fun querySavepoint(savepointRequest: SavepointRequest) = controller.querySavepoint(clusterSelector, savepointRequest, JobContext(job))

    fun createBootstrapJob(bootstrapJob: V1Job) = controller.createBootstrapJob(clusterSelector, bootstrapJob)

    fun deleteBootstrapJob() = controller.deleteBootstrapJob(clusterSelector, getJobName())

    fun stopJob() = controller.stopJob(clusterSelector, JobContext(job))

    fun cancelJob(options: SavepointOptions) = controller.cancelJob(clusterSelector, options, JobContext(job))

    fun isJobCancelled() = FlinkJobStatus.getJobStatus(job) == "CANCELED"

    fun isJobFinished() = FlinkJobStatus.getJobStatus(job) == "FINISHED"

    fun isJobFailed() = FlinkJobStatus.getJobStatus(job) == "FAILED"

    fun isClusterReady(requireFreeSlots: Int) = controller.isClusterReady(clusterSelector, requireFreeSlots)

    fun isClusterHealthy() = controller.isClusterHealthy(clusterSelector)

    fun isClusterStopped() = cluster.status.supervisorStatus == ClusterStatus.Stopped.toString()

    fun isClusterStopping() = cluster.status.supervisorStatus == ClusterStatus.Stopping.toString()

    fun isClusterStarted() = cluster.status.supervisorStatus == ClusterStatus.Started.toString()

    fun isClusterStarting() = cluster.status.supervisorStatus == ClusterStatus.Starting.toString()

    fun isClusterUpdated() = cluster.status.resourceStatus == ResourceStatus.Updated.toString()

    fun refreshStatus(logger: Logger, statusTimestamp: DateTime, actionTimestamp: DateTime, hasFinalizer: Boolean) {
        val savepointMode = job.spec?.savepoint?.savepointMode
        if (FlinkJobStatus.getSavepointMode(job) != savepointMode) {
            FlinkJobStatus.setSavepointMode(job, savepointMode)
        }

        val restartPolicy = job.spec?.savepoint?.restartPolicy
        if (FlinkJobStatus.getRestartPolicy(job) != restartPolicy) {
            FlinkJobStatus.setRestartPolicy(job, restartPolicy)
        }

        val newStatusTimestamp = FlinkJobStatus.getStatusTimestamp(job)

        if (statusTimestamp != newStatusTimestamp) {
            logger.debug("Updating status")
            controller.updateStatus(jobSelector, job)
        }

        val newActionTimestamp = FlinkJobAnnotations.getActionTimestamp(job)

        if (actionTimestamp != newActionTimestamp) {
            logger.debug("Updating annotations")
            controller.updateAnnotations(jobSelector, job)
        }

        val newHasFinalizer = hasFinalizer()

        if (hasFinalizer != newHasFinalizer) {
            logger.debug("Updating finalizers")
            controller.updateFinalizers(jobSelector, job)
        }
    }

    fun hasBeenDeleted() = job.metadata.deletionTimestamp != null

    fun hasFinalizer() = job.metadata.finalizers.orEmpty().contains("finalizer.nextbreakpoint.com")

    fun addFinalizer() {
        val finalizers = job.metadata.finalizers ?: listOf()
        if (!finalizers.contains("finalizer.nextbreakpoint.com")) {
            job.metadata.finalizers = finalizers.plus("finalizer.nextbreakpoint.com")
        }
    }

    fun removeFinalizer() {
        val finalizers = job.metadata.finalizers
        if (finalizers != null && finalizers.contains("finalizer.nextbreakpoint.com")) {
            job.metadata.finalizers = finalizers.minus("finalizer.nextbreakpoint.com")
        }
    }

    fun initializeStatus() {
        val jobParallelism = job.spec?.jobParallelism ?: 1
        FlinkJobStatus.setJobParallelism(job, jobParallelism)

        val savepointPath = job.spec?.savepoint?.savepointPath
        FlinkJobStatus.setSavepointPath(job, savepointPath ?: "")

        val labelSelector = Resource.makeLabelSelector(jobSelector)
        FlinkJobStatus.setLabelSelector(job, labelSelector)

        val savepointMode = job.spec?.savepoint?.savepointMode
        FlinkJobStatus.setSavepointMode(job, savepointMode)

        val restartPolicy = job.spec?.savepoint?.restartPolicy
        FlinkJobStatus.setRestartPolicy(job, restartPolicy)

        val bootstrap = job.spec?.bootstrap
        FlinkJobStatus.setBootstrap(job, bootstrap)

        FlinkJobStatus.setJobStatus(job, "")
    }

    fun initializeAnnotations() {
        FlinkJobAnnotations.setDeleteResources(job, false)
        FlinkJobAnnotations.setWithoutSavepoint(job, false)
        FlinkJobAnnotations.setManualAction(job, ManualAction.NONE)
    }

    fun updateDigests() {
        val bootstrapDigest = Resource.computeDigest(job.spec?.bootstrap)
        FlinkJobStatus.setBootstrapDigest(job, bootstrapDigest)
    }

    fun updateStatus() {
        val jobParallelism = job.spec?.jobParallelism ?: 1
        FlinkJobStatus.setJobParallelism(job, jobParallelism)

        val bootstrap = job.spec?.bootstrap
        FlinkJobStatus.setBootstrap(job, bootstrap)

        FlinkJobStatus.setJobStatus(job, "")
    }

    fun computeChanges(): List<String> {
        val bootstrapDigest = FlinkJobStatus.getBootstrapDigest(job)

        val actualBootstrapDigest = Resource.computeDigest(job.spec?.bootstrap)

        val changes = mutableListOf<String>()

        if (bootstrapDigest != actualBootstrapDigest) {
            changes.add("BOOTSTRAP")
        }

        return changes
    }

    fun setSupervisorStatus(status: JobStatus) {
        FlinkJobStatus.setSupervisorStatus(job, status)
    }

    fun getSupervisorStatus() = FlinkJobStatus.getSupervisorStatus(job)

    fun setResourceStatus(status: ResourceStatus) {
        FlinkJobStatus.setResourceStatus(job, status)
    }

    fun getResourceStatus() = FlinkJobStatus.getResourceStatus(job)

    fun resetAction() {
        FlinkJobAnnotations.setManualAction(job, ManualAction.NONE)
    }

    fun getAction() = FlinkJobAnnotations.getManualAction(job)

    fun setDeleteResources(value: Boolean) {
        FlinkJobAnnotations.setDeleteResources(job, value)
    }

    fun isDeleteResources() = FlinkClusterAnnotations.isDeleteResources(cluster) || FlinkJobAnnotations.isDeleteResources(job)

    fun setWithoutSavepoint(value: Boolean) {
        FlinkJobAnnotations.setWithoutSavepoint(job, value)
    }

    fun isWithoutSavepoint() = FlinkClusterAnnotations.isWithoutSavepoint(cluster) || FlinkJobAnnotations.isWithoutSavepoint(job)

    fun setShouldRestart(value: Boolean) {
        FlinkJobAnnotations.setShouldRestart(job, value)
    }

    fun shouldRestart() = FlinkJobAnnotations.shouldRestart(job)

    fun setSavepointRequest(request: SavepointRequest) {
        FlinkJobStatus.setSavepointRequest(job, request)
    }

    fun getSavepointRequest() = FlinkJobStatus.getSavepointRequest(job)

    fun resetSavepointRequest() {
        FlinkJobStatus.resetSavepointRequest(job)
    }

    fun setSavepointPath(path: String) {
        FlinkJobStatus.setSavepointPath(job, path)
    }

    fun setClusterName(clusterName: String) {
        FlinkJobStatus.setClusterName(job, clusterName)
    }

    fun setClusterHealth(health: String) {
        FlinkJobStatus.setClusterHealth(job, health)
    }

    fun setJobStatus(jobStatus: String) {
        FlinkJobStatus.setJobStatus(job, jobStatus)
    }

    fun getRestartPolicy() = FlinkJobStatus.getRestartPolicy(job)

    fun getSavepointMode() = FlinkJobStatus.getSavepointMode(job)

    fun getSavepointInterval() = FlinkJobConfiguration.getSavepointInterval(job)

    fun getSavepointOptions() = SavepointOptions(targetPath = FlinkJobConfiguration.getSavepointTargetPath(job))

    fun getActionTimestamp() = FlinkJobAnnotations.getActionTimestamp(job)

    fun getStatusTimestamp() = FlinkJobStatus.getStatusTimestamp(job)

    fun doesBootstrapJobExists() = jobResources.bootstrapJob != null

    fun createBootstrapJob(): Result<String?> {
        val savepointPath = FlinkJobStatus.getSavepointPath(job)
        val parallelism = FlinkJobStatus.getJobParallelism(job)

        val resource = BootstrapResourcesDefaultFactory.createBootstrapJob(
            clusterSelector, jobSelector, "flink-operator", getJobName(), job.status.bootstrap, savepointPath, parallelism, controller.isDryRun()
        )

        return createBootstrapJob(resource)
    }

    fun hasJobId() = job.status.jobId != null && job.status.jobId.length > 0

     fun resetJob() {
        FlinkJobStatus.setJobId(job, "")
        FlinkJobStatus.setJobStatus(job, "")
    }

    fun getRequiredTaskSlots() = clusterResources.flinkJobs.values.map { job -> job.status.jobParallelism ?: 0 }.sum()

    fun getJobParallelism() = job.spec.jobParallelism ?: 1

    fun getCurrentJobParallelism() = FlinkJobStatus.getJobParallelism(job)

    fun setCurrentJobParallelism(parallelism: Int) = FlinkJobStatus.setJobParallelism(job, parallelism)

    fun getClusterJobStatus() = controller.getClusterJobStatus(clusterSelector, job.status.jobId)

    fun getJobName() = job.metadata?.labels?.get("jobName") ?: throw RuntimeException("Missing label jobName")
}
