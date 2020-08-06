package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.k8s.crd.V1BootstrapSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobDigest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobStatus
import org.joda.time.DateTime

object FlinkJobStatus {
    fun getStatusTimestamp(flinkJob: V1FlinkJob) : DateTime =
        flinkJob.status?.timestamp ?: DateTime(0)

    fun getSavepointTimestamp(flinkJob: V1FlinkJob) : DateTime =
        flinkJob.status?.savepointTimestamp ?: DateTime(0)

    fun getSavepointRequestTimestamp(flinkJob: V1FlinkJob) : DateTime =
        flinkJob.status?.savepointRequestTimestamp ?: DateTime(0)

    fun getSupervisorStatus(flinkJob: V1FlinkJob) : JobStatus {
        val status = flinkJob.status?.supervisorStatus
        return if (status.isNullOrBlank()) JobStatus.Unknown else JobStatus.valueOf(status)
    }

    fun setSupervisorStatus(flinkJob: V1FlinkJob, status: JobStatus) {
        if (flinkJob.status?.supervisorStatus != status.toString()) {
            ensureState(flinkJob)

            flinkJob.status?.supervisorStatus = status.toString()

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getSavepointRequest(flinkJob: V1FlinkJob) : SavepointRequest? {
        val savepointJobId = flinkJob.status?.savepointJobId
        val savepointTriggerId = flinkJob.status?.savepointTriggerId
        if (savepointJobId == null || savepointTriggerId == null) {
            return null
        }
        if (savepointJobId == "" || savepointTriggerId == "") {
            return null
        }
        return SavepointRequest(jobId = savepointJobId, triggerId = savepointTriggerId)
    }

    fun setSavepointRequest(flinkJob: V1FlinkJob, request: SavepointRequest) {
        ensureState(flinkJob)

        val currentTimeMillis = currentTimeMillis()

        flinkJob.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

        flinkJob.status?.savepointJobId = request.jobId
        flinkJob.status?.savepointTriggerId = request.triggerId

        updateStatusTimestamp(flinkJob, currentTimeMillis)
    }

    fun resetSavepointRequest(flinkJob: V1FlinkJob) {
        ensureState(flinkJob)

        val currentTimeMillis = currentTimeMillis()

        flinkJob.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

        flinkJob.status?.savepointJobId = ""
        flinkJob.status?.savepointTriggerId = ""

        updateStatusTimestamp(flinkJob, currentTimeMillis)
    }

    fun getSavepointPath(flinkJob: V1FlinkJob) : String? =
        if (flinkJob.status?.savepointPath.orEmpty().isBlank()) null else flinkJob.status?.savepointPath

    fun setSavepointPath(flinkJob: V1FlinkJob, path: String) {
        ensureState(flinkJob)

        val currentTimeMillis = currentTimeMillis()

        flinkJob.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

        if ((path.isNotBlank() && flinkJob.status?.savepointPath != path) || (path.isBlank() && flinkJob.status?.savepointPath.orEmpty().isNotBlank())) {
            flinkJob.status?.savepointTimestamp = DateTime(currentTimeMillis)
        }

        flinkJob.status?.savepointPath = path

        updateStatusTimestamp(flinkJob, currentTimeMillis)
    }

    fun setBootstrapDigest(flinkJob: V1FlinkJob, digest: String) {
        ensureState(flinkJob)

        flinkJob.status?.digest?.bootstrap = digest

        updateStatusTimestamp(flinkJob, currentTimeMillis())
    }

    fun getBootstrapDigest(flinkJob: V1FlinkJob): String? =
        flinkJob.status?.digest?.bootstrap

    fun setJobParallelism(flinkJob: V1FlinkJob, jobParallelism: Int) {
        ensureState(flinkJob)

        flinkJob.status?.jobParallelism = jobParallelism

        updateStatusTimestamp(flinkJob, currentTimeMillis())
    }

    fun getJobParallelism(flinkJob: V1FlinkJob): Int =
        flinkJob.status?.jobParallelism ?: 1

    fun setLabelSelector(flinkJob: V1FlinkJob, labelSelector: String) {
        ensureState(flinkJob)

        flinkJob.status?.labelSelector = labelSelector

        updateStatusTimestamp(flinkJob, currentTimeMillis())
    }

    fun getLabelSelector(flinkJob: V1FlinkJob): String? =
        flinkJob.status?.labelSelector

    fun setSavepointMode(flinkJob: V1FlinkJob, savepointMode: String?) {
        ensureState(flinkJob)

        flinkJob.status?.savepointMode = savepointMode

        updateStatusTimestamp(flinkJob, currentTimeMillis())
    }

    fun getSavepointMode(flinkJob: V1FlinkJob): String? =
        flinkJob.status?.savepointMode

    fun setRestartPolicy(flinkJob: V1FlinkJob, restartPolicy: String?) {
        ensureState(flinkJob)

        flinkJob.status?.restartPolicy = restartPolicy

        updateStatusTimestamp(flinkJob, currentTimeMillis())
    }

    fun getRestartPolicy(flinkJob: V1FlinkJob): String? =
        flinkJob.status?.restartPolicy

    fun setBootstrap(flinkJob: V1FlinkJob, bootstrap: V1BootstrapSpec?) {
        ensureState(flinkJob)

        flinkJob.status?.bootstrap = bootstrap

        updateStatusTimestamp(flinkJob, currentTimeMillis())
    }

    fun getBootstrap(flinkJob: V1FlinkJob): V1BootstrapSpec? =
        flinkJob.status?.bootstrap

    private fun updateStatusTimestamp(flinkJob: V1FlinkJob, currentTimeMillis: Long) {
        flinkJob.status?.timestamp = DateTime(currentTimeMillis)
    }

    private fun currentTimeMillis(): Long {
        // this is a hack required for testing
        ensureMillisecondPassed()

        return System.currentTimeMillis()
    }

    private fun ensureMillisecondPassed() {
        try {
            Thread.sleep(1)
        } catch (e: Exception) {
        }
    }

    private fun ensureState(flinkJob: V1FlinkJob) {
        if (flinkJob.status == null) {
            flinkJob.status = V1FlinkJobStatus()
            flinkJob.status.digest = V1FlinkJobDigest()
        }
    }

    fun setResourceStatus(flinkJob: V1FlinkJob, status: ResourceStatus) {
        if (flinkJob.status?.resourceStatus != status.toString()) {
            ensureState(flinkJob)

            flinkJob.status?.resourceStatus = status.toString()

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getResourceStatus(flinkJob: V1FlinkJob): ResourceStatus {
        val status = flinkJob.status?.resourceStatus
        return if (status.isNullOrBlank()) ResourceStatus.Unknown else ResourceStatus.valueOf(status)
    }

    fun setJobStatus(flinkJob: V1FlinkJob, status: String) {
        if (flinkJob.status?.jobStatus != status) {
            ensureState(flinkJob)

            flinkJob.status?.jobStatus = status

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getJobStatus(flinkJob: V1FlinkJob) = flinkJob.status?.jobStatus

    fun setJobId(flinkJob: V1FlinkJob, jobId: String?) {
        ensureState(flinkJob)

        flinkJob.status?.jobId = jobId

        updateStatusTimestamp(flinkJob, currentTimeMillis())
    }

    fun getJobId(flinkJob: V1FlinkJob) = flinkJob.status?.jobId

    fun setClusterName(flinkJob: V1FlinkJob, clusterName: String?) {
        ensureState(flinkJob)

        flinkJob.status?.clusterName = clusterName

        updateStatusTimestamp(flinkJob, currentTimeMillis())
    }

    fun getClusterName(flinkJob: V1FlinkJob) = flinkJob.status?.clusterName

    fun setClusterHealth(flinkJob: V1FlinkJob, clusterHealth: String) {
        if (flinkJob.status?.clusterHealth != clusterHealth) {
            ensureState(flinkJob)

            flinkJob.status?.clusterHealth = clusterHealth

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getClusterHealth(flinkJob: V1FlinkJob) = flinkJob.status?.clusterHealth

//    private fun makeJobDigest(digest: String): V1FlinkJobDigest {
//        val jobDigest = V1FlinkJobDigest()
//        jobDigest.bootstrap = digest
//        return jobDigest
//    }
}