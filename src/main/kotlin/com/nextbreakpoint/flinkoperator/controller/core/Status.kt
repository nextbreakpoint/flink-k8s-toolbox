package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1BootstrapSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flinkoperator.common.crd.V1ResourceDigest
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import org.joda.time.DateTime

object Status {
    fun getClusterStatus(flinkCluster: V1FlinkCluster) : ClusterStatus {
        val status = flinkCluster.status?.clusterStatus
        return if (status.isNullOrBlank()) ClusterStatus.Unknown else ClusterStatus.valueOf(status)
    }

    fun getStatusTimestamp(flinkCluster: V1FlinkCluster) : DateTime =
        flinkCluster.status?.timestamp ?: DateTime(0)

    fun getSavepointPath(flinkCluster: V1FlinkCluster) : String? =
        if (flinkCluster.status?.savepointPath.orEmpty().isBlank()) null else flinkCluster.status?.savepointPath

    fun getSavepointRequest(flinkCluster: V1FlinkCluster) : SavepointRequest? {
        val savepointJobId = flinkCluster.status?.savepointJobId
        val savepointTriggerId = flinkCluster.status?.savepointTriggerId
        if (savepointJobId == null || savepointTriggerId == null) {
            return null
        }
        if (savepointJobId == "" || savepointTriggerId == "") {
            return null
        }
        return SavepointRequest(jobId = savepointJobId, triggerId = savepointTriggerId)
    }

    fun getSavepointTimestamp(flinkCluster: V1FlinkCluster) : DateTime =
        flinkCluster.status?.savepointTimestamp ?: DateTime(0)

    fun getSavepointRequestTimestamp(flinkCluster: V1FlinkCluster) : DateTime =
        flinkCluster.status?.savepointRequestTimestamp ?: DateTime(0)

    fun setSavepointPath(flinkCluster: V1FlinkCluster, path: String) {
        ensureState(flinkCluster)

        val currentTimeMillis = currentTimeMillis()

        flinkCluster.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

        if ((path.isNotBlank() && flinkCluster.status?.savepointPath != path) || (path.isBlank() && flinkCluster.status?.savepointPath.orEmpty().isNotBlank())) {
            flinkCluster.status?.savepointTimestamp = DateTime(currentTimeMillis)
        }

        flinkCluster.status?.savepointPath = path

        updateStatusTimestamp(flinkCluster, currentTimeMillis)
    }

    fun setSavepointRequest(flinkCluster: V1FlinkCluster, request: SavepointRequest) {
        ensureState(flinkCluster)

        val currentTimeMillis = currentTimeMillis()

        flinkCluster.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

        flinkCluster.status?.savepointJobId = request.jobId
        flinkCluster.status?.savepointTriggerId = request.triggerId

        updateStatusTimestamp(flinkCluster, currentTimeMillis)
    }

    fun resetSavepointRequest(flinkCluster: V1FlinkCluster) {
        ensureState(flinkCluster)

        val currentTimeMillis = currentTimeMillis()

        flinkCluster.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

        flinkCluster.status?.savepointJobId = ""
        flinkCluster.status?.savepointTriggerId = ""

        updateStatusTimestamp(flinkCluster, currentTimeMillis)
    }

    fun setClusterStatus(flinkCluster: V1FlinkCluster, status: ClusterStatus) {
        ensureState(flinkCluster)

        flinkCluster.status?.clusterStatus = status.toString()

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun setJobManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.jobManager = digest

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun setTaskManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.taskManager = digest

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun setRuntimeDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.runtime = digest

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun setBootstrapDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.bootstrap = digest

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getJobManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.jobManager

    fun getTaskManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.taskManager

    fun getRuntimeDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.runtime

    fun getBootstrapDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.bootstrap

    fun setTaskManagers(flinkCluster: V1FlinkCluster, taskManagers: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskManagers = taskManagers

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getTaskManagers(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.taskManagers ?: 0

    fun setActiveTaskManagers(flinkCluster: V1FlinkCluster, taskManagers: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.activeTaskManagers = taskManagers

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getActiveTaskManagers(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.activeTaskManagers ?: 0

    fun setJobParallelism(flinkCluster: V1FlinkCluster, jobParallelism: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.jobParallelism = jobParallelism

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getJobParallelism(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.jobParallelism ?: 0

    fun setTaskSlots(flinkCluster: V1FlinkCluster, taskSlots: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskSlots = taskSlots

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getTaskSlots(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.taskSlots ?: 0

    fun setTotalTaskSlots(flinkCluster: V1FlinkCluster, totalTaskSlots: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.totalTaskSlots = totalTaskSlots

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getTotalTaskSlots(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.totalTaskSlots ?: 0

    fun setLabelSelector(flinkCluster: V1FlinkCluster, labelSelector: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.labelSelector = labelSelector

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getLabelSelector(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.labelSelector

    fun setSavepointMode(flinkCluster: V1FlinkCluster, savepointMode: String?) {
        ensureState(flinkCluster)

        flinkCluster.status?.savepointMode = savepointMode

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getSavepointMode(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.savepointMode

    fun setServiceMode(flinkCluster: V1FlinkCluster, serviceMode: String?) {
        ensureState(flinkCluster)

        flinkCluster.status?.serviceMode = serviceMode

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getServiceMode(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.serviceMode

    fun setRestartPolicy(flinkCluster: V1FlinkCluster, restartPolicy: String?) {
        ensureState(flinkCluster)

        flinkCluster.status?.restartPolicy = restartPolicy

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getRestartPolicy(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.restartPolicy

    fun setBootstrap(flinkCluster: V1FlinkCluster, bootstrap: V1BootstrapSpec?) {
        ensureState(flinkCluster)

        flinkCluster.status?.bootstrap = bootstrap

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getBootstrap(flinkCluster: V1FlinkCluster): V1BootstrapSpec? =
        flinkCluster.status?.bootstrap

    private fun updateStatusTimestamp(flinkCluster: V1FlinkCluster, currentTimeMillis: Long) {
        flinkCluster.status?.timestamp = DateTime(currentTimeMillis)
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

    private fun ensureState(flinkCluster: V1FlinkCluster) {
        if (flinkCluster.status == null) {
            flinkCluster.status = V1FlinkClusterStatus()
            flinkCluster.status.digest = V1ResourceDigest()
        }
    }
}