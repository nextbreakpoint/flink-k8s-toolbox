package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1BootstrapSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flinkoperator.common.crd.V1ResourceDigest
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import org.joda.time.DateTime

object Status {
    fun getCurrentTaskStatus(flinkCluster: V1FlinkCluster) : TaskStatus {
        val status = flinkCluster.status?.taskStatus
        return if (status.isNullOrBlank()) TaskStatus.Executing else TaskStatus.valueOf(status)
    }

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

    fun setTaskStatus(flinkCluster: V1FlinkCluster, status: TaskStatus) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskStatus = status.toString()

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun setSavepointPath(flinkCluster: V1FlinkCluster, path: String) {
        ensureState(flinkCluster)

        val currentTimeMillis = currentTimeMillis()

        flinkCluster.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

        if ((path.isNotBlank() && flinkCluster.status?.savepointPath != path) || (path.isBlank() && flinkCluster.status?.savepointPath.orEmpty().isNotBlank())) {
            flinkCluster.status?.savepointTimestamp = DateTime(currentTimeMillis)
        }

        flinkCluster.status?.savepointPath = path

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis)
    }

    fun setSavepointRequest(flinkCluster: V1FlinkCluster, request: SavepointRequest) {
        ensureState(flinkCluster)

        val currentTimeMillis = currentTimeMillis()

        flinkCluster.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

        flinkCluster.status?.savepointJobId = request.jobId
        flinkCluster.status?.savepointTriggerId = request.triggerId

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis)
    }

    fun resetSavepointRequest(flinkCluster: V1FlinkCluster) {
        ensureState(flinkCluster)

        val currentTimeMillis = currentTimeMillis()

        flinkCluster.status?.savepointRequestTimestamp = DateTime(currentTimeMillis)

        flinkCluster.status?.savepointJobId = ""
        flinkCluster.status?.savepointTriggerId = ""

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis)
    }

    fun setClusterStatus(flinkCluster: V1FlinkCluster, status: ClusterStatus) {
        ensureState(flinkCluster)

        flinkCluster.status?.clusterStatus = status.toString()

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun setJobManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.jobManager = digest

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun setTaskManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.taskManager = digest

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun setRuntimeDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.runtime = digest

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun setBootstrapDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.bootstrap = digest

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
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

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun getTaskManagers(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.taskManagers ?: 0

    fun setActiveTaskManagers(flinkCluster: V1FlinkCluster, taskManagers: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.activeTaskManagers = taskManagers

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun getActiveTaskManagers(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.activeTaskManagers ?: 0

    fun setJobParallelism(flinkCluster: V1FlinkCluster, jobParallelism: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.jobParallelism = jobParallelism

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun getJobParallelism(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.jobParallelism ?: 0

    fun setTaskSlots(flinkCluster: V1FlinkCluster, taskSlots: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskSlots = taskSlots

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun getTaskSlots(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.taskSlots ?: 0

    fun setTotalTaskSlots(flinkCluster: V1FlinkCluster, totalTaskSlots: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.totalTaskSlots = totalTaskSlots

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun getTotalTaskSlots(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.totalTaskSlots ?: 0

    fun setLabelSelector(flinkCluster: V1FlinkCluster, labelSelector: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.labelSelector = labelSelector

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun getLabelSelector(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.labelSelector

    fun setSavepointMode(flinkCluster: V1FlinkCluster, savepointMode: String?) {
        ensureState(flinkCluster)

        flinkCluster.status?.savepointMode = savepointMode

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun getSavepointMode(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.savepointMode

    fun setServiceMode(flinkCluster: V1FlinkCluster, serviceMode: String?) {
        ensureState(flinkCluster)

        flinkCluster.status?.serviceMode = serviceMode

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun getServiceMode(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.serviceMode

    fun setJobRestartPolicy(flinkCluster: V1FlinkCluster, jobRestartPolicy: String?) {
        ensureState(flinkCluster)

        flinkCluster.status?.jobRestartPolicy = jobRestartPolicy

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun getJobRestartPolicy(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.jobRestartPolicy

    fun setBootstrap(flinkCluster: V1FlinkCluster, bootstrap: V1BootstrapSpec?) {
        ensureState(flinkCluster)

        flinkCluster.status?.bootstrap = bootstrap

        flinkCluster.status?.timestamp = DateTime(currentTimeMillis())
    }

    fun getBootstrap(flinkCluster: V1FlinkCluster): V1BootstrapSpec? =
        flinkCluster.status?.bootstrap

    private fun currentTimeMillis(): Long {
        try {
            Thread.sleep(1)
        } catch (e : Exception) {
        }
        return System.currentTimeMillis()
    }

    private fun ensureState(flinkCluster: V1FlinkCluster) {
        if (flinkCluster.status == null) {
            flinkCluster.status = V1FlinkClusterStatus()
            flinkCluster.status.digest = V1ResourceDigest()
        }
    }
}