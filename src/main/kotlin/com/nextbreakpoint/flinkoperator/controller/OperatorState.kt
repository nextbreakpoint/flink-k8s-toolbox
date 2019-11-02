package com.nextbreakpoint.flinkoperator.controller

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus

object OperatorState {
    fun hasCurrentTask(flinkCluster: V1FlinkCluster) : Boolean = flinkCluster.status?.tasks?.isNotEmpty() ?: false

    fun getCurrentTask(flinkCluster: V1FlinkCluster) : OperatorTask =
        flinkCluster.status?.tasks?.filter { it.isNotBlank() }?.map { OperatorTask.valueOf(it) }?.firstOrNull() ?: OperatorTask.CLUSTER_HALTED

    fun getCurrentTaskStatus(flinkCluster: V1FlinkCluster) : TaskStatus {
        val status = flinkCluster.status?.taskStatus
        return if (status.isNullOrBlank()) TaskStatus.EXECUTING else TaskStatus.valueOf(status)
    }

    fun getClusterStatus(flinkCluster: V1FlinkCluster) : ClusterStatus {
        val status = flinkCluster.status?.clusterStatus
        return if (status.isNullOrBlank()) ClusterStatus.UNKNOWN else ClusterStatus.valueOf(status)
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

        val task = flinkCluster.status?.tasks?.firstOrNull() ?: OperatorTask.CLUSTER_HALTED.toString()

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

    fun setSavepointPath(flinkCluster: V1FlinkCluster, path: String) {
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

        flinkCluster.status?.digestOfJobManager = digest

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setTaskManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digestOfTaskManager = digest

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setFlinkImageDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digestOfFlinkImage = digest

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun setFlinkJobDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digestOfFlinkJob= digest

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun getJobManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digestOfJobManager

    fun getTaskManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digestOfTaskManager

    fun getFlinkImageDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digestOfFlinkImage

    fun getFlinkJobDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digestOfFlinkJob

    fun setOperatorTaskAttempts(flinkCluster: V1FlinkCluster, attempts: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskAttempts = attempts

        flinkCluster.status?.timestamp = currentTimeMillis()
    }

    fun getOperatorTaskAttempts(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.taskAttempts ?: 0

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
            flinkCluster.metadata?.annotations?.let { annotations ->
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_TIMESTAMP)?.let {
                    flinkCluster.status?.timestamp = it.toLong()
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_TASKS)?.let {
                    flinkCluster.status?.tasks = it.split(" ").toTypedArray()
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_TASK_STATUS)?.let {
                    setTaskStatus(flinkCluster, TaskStatus.valueOf(it))
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_TASK_ATTEMPTS)?.let {
                    setOperatorTaskAttempts(flinkCluster, it.toInt())
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_CLUSTER_STATUS)?.let {
                    setClusterStatus(flinkCluster, ClusterStatus.valueOf(it))
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_IMAGE_DIGEST)?.let {
                    flinkCluster.status?.digestOfFlinkImage = it
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_JOB_DIGEST)?.let {
                    flinkCluster.status?.digestOfFlinkJob = it
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_JOBMANAGER_DIGEST)?.let {
                    flinkCluster.status?.digestOfJobManager = it
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_TASKMANAGER_DIGEST)?.let {
                    flinkCluster.status?.digestOfTaskManager = it
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_SAVEPOINT_PATH)?.let {
                    flinkCluster.status?.savepointPath = it
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_SAVEPOINT_REQUEST)?.let {
                    val request = Gson().fromJson(it, SavepointRequest::class.java)
                    flinkCluster.status?.savepointJobId = request.jobId
                    flinkCluster.status?.savepointTriggerId = request.triggerId
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_SAVEPOINT_TIMESTAMP)?.let {
                    flinkCluster.status?.savepointTimestamp = it.toLong()
                }
            }
        }
    }
}