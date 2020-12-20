package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.RestartPolicy
import com.nextbreakpoint.flink.common.SavepointMode
import com.nextbreakpoint.flink.common.SavepointOptions
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.k8s.common.FlinkClusterAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkJobAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkJobConfiguration
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.factory.BootstrapResourcesDefaultFactory
import org.joda.time.DateTime
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.max
import kotlin.math.min

class JobController(
    val namespace: String,
    val clusterName: String,
    val jobName: String,
    val pollingInterval: Long,
    private val controller: Controller,
    private val clusterResources: ClusterResources,
    private val jobResources: JobResources,
    private val job: V1FlinkJob
) {
    fun timeSinceLastUpdateInSeconds() = (controller.currentTimeMillis() - FlinkJobStatus.getStatusTimestamp(job).millis) / 1000L

    fun timeSinceLastSavepointRequestInSeconds() = (controller.currentTimeMillis() - FlinkJobStatus.getSavepointRequestTimestamp(job).millis) / 1000L

    fun triggerSavepoint(options: SavepointOptions) = controller.triggerSavepoint(namespace, clusterName, jobName, options, JobContext(job))

    fun querySavepoint(savepointRequest: SavepointRequest) = controller.querySavepoint(namespace, clusterName, jobName, savepointRequest, JobContext(job))

    fun stopJob() = controller.stopJob(namespace, clusterName, jobName, JobContext(job))

    fun cancelJob(options: SavepointOptions) = controller.cancelJob(namespace, clusterName, jobName, options, JobContext(job))

    fun isJobCancelled() = FlinkJobStatus.getJobStatus(job) == "CANCELED"

    fun isJobFinished() = FlinkJobStatus.getJobStatus(job) == "FINISHED"

    fun isJobFailed() = FlinkJobStatus.getJobStatus(job) == "FAILED"

    fun isJobRunning() = FlinkJobStatus.getJobStatus(job) == "RUNNING"

    fun isClusterReady(requireFreeSlots: Int) = controller.isClusterReady(namespace, clusterName, requireFreeSlots)

    fun isClusterHealthy() = controller.isClusterHealthy(namespace, clusterName)

    fun isClusterStopped() = clusterResources.flinkCluster?.status?.supervisorStatus == ClusterStatus.Stopped.toString()

    fun isClusterStopping() = clusterResources.flinkCluster?.status?.supervisorStatus == ClusterStatus.Stopping.toString()

    fun isClusterStarted() = clusterResources.flinkCluster?.status?.supervisorStatus == ClusterStatus.Started.toString()

    fun isClusterStarting() = clusterResources.flinkCluster?.status?.supervisorStatus == ClusterStatus.Starting.toString()

    fun isClusterTerminated() = clusterResources.flinkCluster?.status?.supervisorStatus == ClusterStatus.Terminated.toString()

    fun isClusterUpdated() = clusterResources.flinkCluster?.status?.resourceStatus == ResourceStatus.Updated.toString()

    fun refreshStatus(logger: Logger, statusTimestamp: DateTime, actionTimestamp: DateTime, hasFinalizer: Boolean) {
        val savepointMode = SavepointMode.valueOf(job.spec.savepoint.savepointMode)
        FlinkJobStatus.setSavepointMode(job, savepointMode)

        val restartPolicy = RestartPolicy.valueOf(job.spec.restart.restartPolicy)
        FlinkJobStatus.setRestartPolicy(job, restartPolicy)

        val newStatusTimestamp = FlinkJobStatus.getStatusTimestamp(job)

        val resourceName = "$clusterName-$jobName"

        if (statusTimestamp != newStatusTimestamp) {
            logger.log(Level.FINE, "Updating status")
            controller.updateStatus(namespace, resourceName, job)
        }

        val newActionTimestamp = FlinkJobAnnotations.getActionTimestamp(job)

        if (actionTimestamp != newActionTimestamp) {
            logger.log(Level.FINE, "Updating annotations")
            controller.updateAnnotations(namespace, resourceName, job)
        }

        val newHasFinalizer = hasFinalizer()

        if (hasFinalizer != newHasFinalizer) {
            logger.log(Level.FINE, "Updating finalizers")
            controller.updateFinalizers(namespace, resourceName, job)
        }
    }

    fun hasBeenDeleted() = job.metadata.deletionTimestamp != null

    fun hasFinalizer() = job.metadata.finalizers.orEmpty().contains(Resource.SUPERVISOR_FINALIZER_VALUE)

    fun addFinalizer() {
        val finalizers = job.metadata.finalizers ?: listOf()
        if (!finalizers.contains(Resource.SUPERVISOR_FINALIZER_VALUE)) {
            job.metadata.finalizers = finalizers.plus(Resource.SUPERVISOR_FINALIZER_VALUE)
        }
    }

    fun removeFinalizer() {
        val finalizers = job.metadata.finalizers
        if (finalizers != null && finalizers.contains(Resource.SUPERVISOR_FINALIZER_VALUE)) {
            job.metadata.finalizers = finalizers.minus(Resource.SUPERVISOR_FINALIZER_VALUE)
        }
    }

    fun initializeStatus() {
        if (FlinkJobStatus.getSavepointPath(job) == null) {
            val savepointPath = job.spec.savepoint.savepointPath
            FlinkJobStatus.setSavepointPath(job, savepointPath ?: "")
        }

        val labelSelector = Resource.makeLabelSelector(clusterName, jobName)
        FlinkJobStatus.setLabelSelector(job, labelSelector)

        updateStatus()
    }

    fun initializeAnnotations() {
        FlinkJobAnnotations.setDeleteResources(job, false)
        FlinkJobAnnotations.setWithoutSavepoint(job, false)
        FlinkJobAnnotations.setRequestedAction(job, Action.NONE)
    }

    fun updateDigests() {
        val bootstrapDigest = Resource.computeDigest(job.spec.bootstrap)
        FlinkJobStatus.setBootstrapDigest(job, bootstrapDigest)

        val savepointDigest = Resource.computeDigest(job.spec.savepoint)
        FlinkJobStatus.setSavepointDigest(job, savepointDigest)

        val restartDigest = Resource.computeDigest(job.spec.restart)
        FlinkJobStatus.setRestartDigest(job, restartDigest)
    }

    fun updateStatus() {
        FlinkJobStatus.setJobParallelism(job, getDeclaredJobParallelism())

        val savepointMode = SavepointMode.valueOf(job.spec.savepoint.savepointMode)
        FlinkJobStatus.setSavepointMode(job, savepointMode)

        val restartPolicy = RestartPolicy.valueOf(job.spec.restart.restartPolicy)
        FlinkJobStatus.setRestartPolicy(job, restartPolicy)

        FlinkJobStatus.setJobStatus(job, "")
    }

    fun computeChanges(): List<String> {
        val bootstrapDigest = FlinkJobStatus.getBootstrapDigest(job)

        val actualBootstrapDigest = Resource.computeDigest(job.spec.bootstrap)

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
        FlinkJobAnnotations.setRequestedAction(job, Action.NONE)
    }

    fun getAction() = FlinkJobAnnotations.getRequestedAction(job)

    fun setDeleteResources(value: Boolean) {
        FlinkJobAnnotations.setDeleteResources(job, value)
    }

    fun isDeleteResources() = FlinkJobAnnotations.isDeleteResources(job)

    fun setWithoutSavepoint(value: Boolean) {
        FlinkJobAnnotations.setWithoutSavepoint(job, value)
    }

    fun isWithoutSavepoint() = clusterIsWithoutSavepoint() || FlinkJobAnnotations.isWithoutSavepoint(job)

    fun setShouldRestart(value: Boolean) {
        FlinkJobAnnotations.setShouldRestart(job, value)
    }

    fun shouldRestart() = FlinkJobAnnotations.shouldRestart(job)

    fun shouldCreateSavepoint() = FlinkJobStatus.getSavepointMode(job) == SavepointMode.Automatic

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

    fun getRestartDelay() = FlinkJobConfiguration.getRestartDelay(job)

    fun getRestartTimeout() = FlinkJobConfiguration.getRestartTimeout(job)

    fun getSavepointMode() = FlinkJobStatus.getSavepointMode(job)

    fun getSavepointInterval() = FlinkJobConfiguration.getSavepointInterval(job)

    fun getSavepointOptions() = SavepointOptions(targetPath = FlinkJobConfiguration.getSavepointTargetPath(job))

    fun getActionTimestamp() = FlinkJobAnnotations.getActionTimestamp(job)

    fun getStatusTimestamp() = FlinkJobStatus.getStatusTimestamp(job)

    fun doesBootstrapJobExists() = jobResources.bootstrapJob != null

    fun createBootstrapJob(): Result<String?> {
        val resource = BootstrapResourcesDefaultFactory.createBootstrapJob(
            namespace,
            Resource.RESOURCE_OWNER,
            clusterName,
            jobName,
            job.spec.bootstrap,
            getCurrentSavepointPath(),
            getCurrentJobParallelism(),
            controller.isDryRun()
        )

        FlinkJobStatus.updateStatusTimestamp(job, controller.currentTimeMillis())

        return controller.createBootstrapJob(namespace, clusterName, jobName, resource)
    }

    fun deleteBootstrapJob(): Result<Void?> {
        val name = jobResources.bootstrapJob?.metadata?.name
        if (name != null && jobResources.bootstrapJob?.metadata?.deletionTimestamp == null) {
            FlinkJobStatus.updateStatusTimestamp(job, controller.currentTimeMillis())

            return controller.deleteBootstrapJob(namespace, clusterName, jobName, name)
        } else {
            return Result(ResultStatus.ERROR, null)
        }
    }

    fun hasJobId() = job.status?.jobId?.isNotEmpty() ?: false

    fun resetJob() {
        FlinkJobStatus.setJobId(job, "")
        FlinkJobStatus.setJobStatus(job, "")
    }

    fun getRequiredTaskSlots() = clusterResources.flinkJobs
        .filter { job -> activeStatus.contains(job.status.supervisorStatus) }
        .map { job -> job.status?.jobParallelism ?: 0 }.sum()

    fun getDeclaredJobParallelism() = min(max(job.spec.jobParallelism ?: 0, job.spec.minJobParallelism ?: 0), job.spec.maxJobParallelism ?: 32)

    fun getCurrentJobParallelism() = FlinkJobStatus.getJobParallelism(job)

    fun setCurrentJobParallelism(parallelism: Int) = FlinkJobStatus.setJobParallelism(job, parallelism)

    fun getCurrentSavepointPath() = FlinkJobStatus.getSavepointPath(job)

    fun getJobStatus() = if (job.status != null) controller.getJobStatus(namespace, clusterName, jobName, job.status.jobId) else Result(ResultStatus.ERROR, null)

    fun isJobSuspended() = getCurrentJobParallelism() == 0

    private val activeStatus = setOf(JobStatus.Starting.toString(), JobStatus.Started.toString())

    private fun clusterIsWithoutSavepoint() = clusterResources.flinkCluster?.let { FlinkClusterAnnotations.isWithoutSavepoint(clusterResources.flinkCluster) } ?: false
}
