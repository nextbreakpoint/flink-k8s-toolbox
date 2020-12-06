package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.RestartPolicy
import com.nextbreakpoint.flink.common.SavepointMode
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobDigest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobStatus
import org.joda.time.DateTime

object FlinkJobStatus {
    @JvmStatic
    private val initialisedTimestamp = currentTimeMillis()

    fun getStatusTimestamp(flinkJob: V1FlinkJob) : DateTime =
        flinkJob.status?.timestamp ?: DateTime(initialisedTimestamp)

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
        if (flinkJob.status?.savepointJobId != request.jobId || flinkJob.status?.savepointTriggerId != request.triggerId) {
            ensureState(flinkJob)

            val currentTimeMillis = currentTimeMillis()

            flinkJob.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

            flinkJob.status?.savepointJobId = request.jobId
            flinkJob.status?.savepointTriggerId = request.triggerId

            updateStatusTimestamp(flinkJob, currentTimeMillis)
        }
    }

    fun resetSavepointRequest(flinkJob: V1FlinkJob) {
        setSavepointRequest(flinkJob, SavepointRequest("", ""))
    }

    fun getSavepointPath(flinkJob: V1FlinkJob) : String? =
        if (flinkJob.status?.savepointPath.isNullOrBlank()) null else flinkJob.status?.savepointPath

    fun setSavepointPath(flinkJob: V1FlinkJob, path: String) {
        if (flinkJob.status?.savepointPath != path) {
            ensureState(flinkJob)

            val currentTimeMillis = currentTimeMillis()

            flinkJob.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

            if ((path.isNotBlank() && flinkJob.status?.savepointPath != path) || (path.isBlank() && flinkJob.status?.savepointPath.orEmpty().isNotBlank())) {
                flinkJob.status?.savepointTimestamp = DateTime(currentTimeMillis)
            }

            flinkJob.status?.savepointPath = path

            updateStatusTimestamp(flinkJob, currentTimeMillis)
        }
    }

    fun getBootstrapDigest(flinkJob: V1FlinkJob): String? =
        flinkJob.status?.digest?.bootstrap

    fun setBootstrapDigest(flinkJob: V1FlinkJob, digest: String) {
        if (flinkJob.status?.digest?.bootstrap != digest) {
            ensureState(flinkJob)

            flinkJob.status?.digest?.bootstrap = digest

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getSavepointDigest(flinkJob: V1FlinkJob): String? =
        flinkJob.status?.digest?.savepoint

    fun setSavepointDigest(flinkJob: V1FlinkJob, digest: String) {
        if (flinkJob.status?.digest?.savepoint != digest) {
            ensureState(flinkJob)

            flinkJob.status?.digest?.savepoint = digest

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getJobParallelism(flinkJob: V1FlinkJob): Int =
        flinkJob.status?.jobParallelism ?: 1

    fun setJobParallelism(flinkJob: V1FlinkJob, jobParallelism: Int) {
        if (flinkJob.status?.jobParallelism != jobParallelism) {
            ensureState(flinkJob)

            flinkJob.status?.jobParallelism = jobParallelism

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getLabelSelector(flinkJob: V1FlinkJob): String? =
        flinkJob.status?.labelSelector

    fun setLabelSelector(flinkJob: V1FlinkJob, labelSelector: String) {
        if (flinkJob.status?.labelSelector != labelSelector) {
            ensureState(flinkJob)

            flinkJob.status?.labelSelector = labelSelector

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getSavepointMode(flinkJob: V1FlinkJob): SavepointMode {
        val savepointMode = flinkJob.status?.savepointMode
        return if (savepointMode.isNullOrBlank()) SavepointMode.Automatic else SavepointMode.valueOf(savepointMode)
    }

    fun setSavepointMode(flinkJob: V1FlinkJob, savepointMode: SavepointMode) {
        if (flinkJob.status?.savepointMode != savepointMode.toString()) {
            ensureState(flinkJob)

            flinkJob.status?.savepointMode = savepointMode.toString()

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getRestartPolicy(flinkJob: V1FlinkJob): RestartPolicy {
        val restartPolicy = flinkJob.status?.restartPolicy
        return if (restartPolicy.isNullOrBlank()) RestartPolicy.Always else RestartPolicy.valueOf(restartPolicy)
    }

    fun setRestartPolicy(flinkJob: V1FlinkJob, restartPolicy: RestartPolicy) {
        if (flinkJob.status?.restartPolicy != restartPolicy.toString()) {
            ensureState(flinkJob)

            flinkJob.status?.restartPolicy = restartPolicy.toString()

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getResourceStatus(flinkJob: V1FlinkJob): ResourceStatus {
        val status = flinkJob.status?.resourceStatus
        return if (status.isNullOrBlank()) ResourceStatus.Unknown else ResourceStatus.valueOf(status)
    }

    fun setResourceStatus(flinkJob: V1FlinkJob, status: ResourceStatus) {
        if (flinkJob.status?.resourceStatus != status.toString()) {
            ensureState(flinkJob)

            flinkJob.status?.resourceStatus = status.toString()

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getJobStatus(flinkJob: V1FlinkJob) = flinkJob.status?.jobStatus

    fun setJobStatus(flinkJob: V1FlinkJob, status: String) {
        if (flinkJob.status?.jobStatus != status) {
            ensureState(flinkJob)

            flinkJob.status?.jobStatus = status

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getJobId(flinkJob: V1FlinkJob) = flinkJob.status?.jobId

    fun setJobId(flinkJob: V1FlinkJob, jobId: String?) {
        if (flinkJob.status?.jobId != jobId) {
            ensureState(flinkJob)

            flinkJob.status?.jobId = jobId

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getClusterName(flinkJob: V1FlinkJob) = flinkJob.status?.clusterName

    fun setClusterName(flinkJob: V1FlinkJob, clusterName: String?) {
        if (flinkJob.status?.clusterName != clusterName) {
            ensureState(flinkJob)

            flinkJob.status?.clusterName = clusterName

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    fun getClusterHealth(flinkJob: V1FlinkJob) = flinkJob.status?.clusterHealth

    fun setClusterHealth(flinkJob: V1FlinkJob, clusterHealth: String) {
        if (flinkJob.status?.clusterHealth != clusterHealth) {
            ensureState(flinkJob)

            flinkJob.status?.clusterHealth = clusterHealth

            updateStatusTimestamp(flinkJob, currentTimeMillis())
        }
    }

    private fun updateStatusTimestamp(flinkJob: V1FlinkJob, currentTimeMillis: Long) {
        flinkJob.status?.timestamp = DateTime(currentTimeMillis)
    }

    @JvmStatic
    private fun currentTimeMillis(): Long {
        // this is a hack required for testing
        ensureMillisecondPassed()

        return System.currentTimeMillis()
    }

    @JvmStatic
    private fun ensureMillisecondPassed() {
        try {
            Thread.sleep(1)
        } catch (e: Exception) {
        }
    }

    private fun ensureState(flinkJob: V1FlinkJob) {
        if (flinkJob.status == null) {
            flinkJob.status = V1FlinkJobStatus.builder().build()
            flinkJob.status.digest = V1FlinkJobDigest.builder().build()
        }
    }
}