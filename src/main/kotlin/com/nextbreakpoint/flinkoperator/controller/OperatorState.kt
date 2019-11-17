package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flinkoperator.common.crd.V1ResourceDigest
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus

object OperatorState {
    fun hasCurrentTask(flinkCluster: V1FlinkCluster) : Boolean = flinkCluster.status?.tasks?.isNotEmpty() ?: false

    fun getCurrentTask(flinkCluster: V1FlinkCluster) : OperatorTask =
        flinkCluster.status?.tasks?.filter { it.isNotBlank() }?.map { OperatorTask.valueOf(it) }?.firstOrNull() ?: OperatorTask.ClusterHalted

    fun getCurrentTaskStatus(flinkCluster: V1FlinkCluster) : TaskStatus {
        val status = flinkCluster.status?.taskStatus
        return if (status.isNullOrBlank()) TaskStatus.Executing else TaskStatus.valueOf(status)
    }

    fun getClusterStatus(flinkCluster: V1FlinkCluster) : ClusterStatus {
        val status = flinkCluster.status?.clusterStatus
        return if (status.isNullOrBlank()) ClusterStatus.Unknown else ClusterStatus.valueOf(status)
    }

    fun getOperatorTimestamp(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.status?.timestamp?.toLong() ?: 0

    fun getSavepointPath(flinkCluster: V1FlinkCluster) : String? =
        flinkCluster.status?.savepointPath

    fun getSavepointRequest(flinkCluster: V1FlinkCluster) : SavepointRequest? {
        val savepointJobId = flinkCluster.status?.savepointJobId
        if (savepointJobId == null || savepointJobId == "") {
            return null
        }
        val savepointTriggerId = flinkCluster.status?.savepointTriggerId
        if (savepointTriggerId == null || savepointTriggerId == "") {
            return null
        }
        return SavepointRequest(jobId = savepointJobId, triggerId = savepointTriggerId)
    }

    fun getSavepointTimestamp(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.status?.savepointTimestamp?.toLong() ?: 0

    fun getNextOperatorTask(flinkCluster: V1FlinkCluster) : OperatorTask? =
        flinkCluster.status?.tasks?.drop(1)?.map { OperatorTask.valueOf(it) }?.firstOrNull()

    fun selectNextTask(flinkCluster: V1FlinkCluster) {
        ensureState(flinkCluster)

        val task = flinkCluster.status?.tasks?.firstOrNull() ?: OperatorTask.ClusterHalted.toString()

        val tasks = flinkCluster.status?.tasks?.drop(1).orEmpty()

        flinkCluster.status?.tasks = if (tasks.isEmpty()) arrayOf(task) else tasks.toTypedArray()

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun appendTasks(flinkCluster: V1FlinkCluster, tasks: List<OperatorTask>) {
        ensureState(flinkCluster)

        val currentTask = flinkCluster.status?.tasks?.toList().orEmpty().toMutableList()

        val newTasks = tasks.map { it.toString() }.toList()

        flinkCluster.status?.tasks = currentTask.plus(newTasks).toTypedArray()

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun resetTasks(flinkCluster: V1FlinkCluster, tasks: List<OperatorTask>) {
        ensureState(flinkCluster)

        val newTasks = tasks.map { it.toString() }.toList()

        flinkCluster.status?.tasks = newTasks.toTypedArray()

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setTaskStatus(flinkCluster: V1FlinkCluster, status: TaskStatus) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskStatus = status.toString()

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setSavepointPath(flinkCluster: V1FlinkCluster, path: String?) {
        ensureState(flinkCluster)

        flinkCluster.status?.savepointTimestamp = currentTimeMillis()

        flinkCluster.status?.savepointPath = path

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setSavepointRequest(flinkCluster: V1FlinkCluster, request: SavepointRequest) {
        ensureState(flinkCluster)

        flinkCluster.status?.savepointTimestamp = currentTimeMillis()

        flinkCluster.status?.savepointJobId = request.jobId
        flinkCluster.status?.savepointTriggerId = request.triggerId

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun updateSavepointTimestamp(flinkCluster: V1FlinkCluster) {
        ensureState(flinkCluster)

        flinkCluster.status?.savepointTimestamp = currentTimeMillis()

        flinkCluster.status?.savepointJobId = null
        flinkCluster.status?.savepointTriggerId = null

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setClusterStatus(flinkCluster: V1FlinkCluster, status: ClusterStatus) {
        ensureState(flinkCluster)

        flinkCluster.status?.clusterStatus = status.toString()

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setJobManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.jobManager = digest

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setTaskManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.taskManager = digest

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setRuntimeDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.runtime = digest

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setBootstrapDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.bootstrap = digest

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun getJobManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.jobManager

    fun getTaskManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.taskManager

    fun getRuntimeDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.runtime

    fun getBootstrapDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.bootstrap

    fun setTaskAttempts(flinkCluster: V1FlinkCluster, attempts: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskAttempts = attempts

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun getTaskAttempts(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.taskAttempts ?: 0

    fun setTaskManagers(flinkCluster: V1FlinkCluster, taskManagers: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskManagers = taskManagers

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun getTaskManagers(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.taskManagers ?: 0

    fun setActiveTaskManagers(flinkCluster: V1FlinkCluster, taskManagers: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.activeTaskManagers = taskManagers

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun getActiveTaskManagers(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.activeTaskManagers ?: 0

    fun setJobParallelism(flinkCluster: V1FlinkCluster, jobParallelism: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.jobParallelism = jobParallelism

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun getJobParallelism(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.jobParallelism ?: 0

    fun setTaskSlots(flinkCluster: V1FlinkCluster, taskSlots: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskSlots = taskSlots

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun getTaskSlots(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.taskSlots ?: 0

    fun setTotalTaskSlots(flinkCluster: V1FlinkCluster, totalTaskSlots: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.totalTaskSlots = totalTaskSlots

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun getTotalTaskSlots(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.totalTaskSlots ?: 0

    fun setLabelSelector(flinkCluster: V1FlinkCluster, labelSelector: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.labelSelector = labelSelector

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun getLabelSelector(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.labelSelector

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