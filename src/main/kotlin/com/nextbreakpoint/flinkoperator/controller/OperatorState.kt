package com.nextbreakpoint.flinkoperator.controller

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterState
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus

object OperatorState {
    fun hasCurrentTask(flinkCluster: V1FlinkCluster) : Boolean = flinkCluster.state?.tasks?.isNotEmpty() ?: false

    fun getCurrentTask(flinkCluster: V1FlinkCluster) : OperatorTask =
        flinkCluster.state?.tasks?.filter { it.isNotBlank() }?.map { OperatorTask.valueOf(it) }?.firstOrNull() ?: OperatorTask.CLUSTER_HALTED

    fun getCurrentTaskStatus(flinkCluster: V1FlinkCluster) : TaskStatus {
        val status = flinkCluster.state?.taskStatus
        return if (status.isNullOrBlank()) TaskStatus.EXECUTING else TaskStatus.valueOf(status)
    }

    fun getClusterStatus(flinkCluster: V1FlinkCluster) : ClusterStatus {
        val status = flinkCluster.state?.clusterStatus
        return if (status.isNullOrBlank()) ClusterStatus.UNKNOWN else ClusterStatus.valueOf(status)
    }

    fun getOperatorTimestamp(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.state?.timestamp?.toLong() ?: 0

    fun getSavepointPath(flinkCluster: V1FlinkCluster) : String? =
        flinkCluster.state?.savepointPath

    fun getSavepointRequest(flinkCluster: V1FlinkCluster) : SavepointRequest? {
        val savepointJobId = flinkCluster.state?.savepointJobId
        if (savepointJobId == null || savepointJobId == "") {
            return null
        }
        val savepointTriggerId = flinkCluster.state?.savepointTriggerId
        if (savepointTriggerId == null || savepointTriggerId == "") {
            return null
        }
        return SavepointRequest(jobId = savepointJobId, triggerId = savepointTriggerId)
    }

    fun getSavepointTimestamp(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.state?.savepointTimestamp?.toLong() ?: 0

    fun getNextOperatorTask(flinkCluster: V1FlinkCluster) : OperatorTask? =
        flinkCluster.state?.tasks?.drop(1)?.map { OperatorTask.valueOf(it) }?.firstOrNull()

    fun selectNextTask(flinkCluster: V1FlinkCluster) {
        ensureState(flinkCluster)

        val task = flinkCluster.state?.tasks?.firstOrNull() ?: OperatorTask.CLUSTER_HALTED.toString()

        val tasks = flinkCluster.state?.tasks?.drop(1).orEmpty()

        flinkCluster.state?.tasks = if (tasks.isEmpty()) arrayOf(task) else tasks.toTypedArray()

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun appendTasks(flinkCluster: V1FlinkCluster, tasks: List<OperatorTask>) {
        ensureState(flinkCluster)

        val currentTask = flinkCluster.state?.tasks?.toList().orEmpty().toMutableList()

        val newTasks = tasks.map { it.toString() }.toList()

        flinkCluster.state?.tasks = currentTask.plus(newTasks).toTypedArray()

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun resetTasks(flinkCluster: V1FlinkCluster, tasks: List<OperatorTask>) {
        ensureState(flinkCluster)

        val newTasks = tasks.map { it.toString() }.toList()

        flinkCluster.state?.tasks = newTasks.toTypedArray()

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun setTaskStatus(flinkCluster: V1FlinkCluster, status: TaskStatus) {
        ensureState(flinkCluster)

        flinkCluster.state?.taskStatus = status.toString()

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun setSavepointPath(flinkCluster: V1FlinkCluster, path: String) {
        ensureState(flinkCluster)

        flinkCluster.state?.savepointTimestamp = currentTimeMillis()

        flinkCluster.state?.savepointPath = path

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun setSavepointRequest(flinkCluster: V1FlinkCluster, request: SavepointRequest) {
        ensureState(flinkCluster)

        flinkCluster.state?.savepointTimestamp = currentTimeMillis()

        flinkCluster.state?.savepointJobId = request.jobId
        flinkCluster.state?.savepointTriggerId = request.triggerId

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun updateSavepointTimestamp(flinkCluster: V1FlinkCluster) {
        ensureState(flinkCluster)

        flinkCluster.state?.savepointTimestamp = currentTimeMillis()

        flinkCluster.state?.savepointJobId = null
        flinkCluster.state?.savepointTriggerId = null

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun setClusterStatus(flinkCluster: V1FlinkCluster, status: ClusterStatus) {
        ensureState(flinkCluster)

        flinkCluster.state?.clusterStatus = status.toString()

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun setJobManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.state?.digestOfJobManager = digest

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun setTaskManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.state?.digestOfTaskManager = digest

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun setFlinkImageDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.state?.digestOfFlinkImage = digest

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun setFlinkJobDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.state?.digestOfFlinkJob= digest

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun getJobManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.state?.digestOfJobManager

    fun getTaskManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.state?.digestOfTaskManager

    fun getFlinkImageDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.state?.digestOfFlinkImage

    fun getFlinkJobDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.state?.digestOfFlinkJob

    fun setOperatorTaskAttempts(flinkCluster: V1FlinkCluster, attempts: Int) {
        ensureState(flinkCluster)

        flinkCluster.state?.taskAttempts = attempts

        flinkCluster.state?.timestamp = currentTimeMillis()
    }

    fun getOperatorTaskAttempts(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.state?.taskAttempts ?: 0

    private fun currentTimeMillis(): Long {
        try {
            Thread.sleep(1)
        } catch (e : Exception) {
        }
        return System.currentTimeMillis()
    }

    private fun ensureState(flinkCluster: V1FlinkCluster) {
        if (flinkCluster.state == null) {
            flinkCluster.state = V1FlinkClusterState()
            flinkCluster.metadata?.annotations?.let { annotations ->
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_TIMESTAMP)?.let {
                    flinkCluster.state?.timestamp = it.toLong()
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_TASKS)?.let {
                    flinkCluster.state?.tasks = it.split(" ").toTypedArray()
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
                    flinkCluster.state?.digestOfFlinkImage = it
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_JOB_DIGEST)?.let {
                    flinkCluster.state?.digestOfFlinkJob = it
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_JOBMANAGER_DIGEST)?.let {
                    flinkCluster.state?.digestOfJobManager = it
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_TASKMANAGER_DIGEST)?.let {
                    flinkCluster.state?.digestOfTaskManager = it
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_SAVEPOINT_PATH)?.let {
                    flinkCluster.state?.savepointPath = it
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_SAVEPOINT_REQUEST)?.let {
                    val request = Gson().fromJson(it, SavepointRequest::class.java)
                    flinkCluster.state?.savepointJobId = request.jobId
                    flinkCluster.state?.savepointTriggerId = request.triggerId
                }
                annotations.get(OperatorAnnotations.FLINK_OPERATOR_SAVEPOINT_TIMESTAMP)?.let {
                    flinkCluster.state?.savepointTimestamp = it.toLong()
                }
            }
        }
    }
}