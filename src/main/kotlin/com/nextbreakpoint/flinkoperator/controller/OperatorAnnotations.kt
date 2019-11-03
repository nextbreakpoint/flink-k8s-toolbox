package com.nextbreakpoint.flinkoperator.controller

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus

object OperatorAnnotations {
    val MANUAL_ACTION                       = "flink-operator/manual-action"
    val ACTION_TIMESTAMP                    = "flink-operator/action-timestamp"
    val WITHOUT_SAVEPOINT                   = "flink-operator/without-savepoint"

    fun getActionTimestamp(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.metadata?.annotations?.get(ACTION_TIMESTAMP)?.toLong() ?: 0

    fun setActionTimestamp(flinkCluster: V1FlinkCluster, timestamp: Long) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[ACTION_TIMESTAMP] = timestamp.toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun setManualActionAndWithoutSavepoint(flinkCluster: V1FlinkCluster, manualAction: String, withoutSavepoint: Boolean) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[MANUAL_ACTION] = manualAction
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        annotations[WITHOUT_SAVEPOINT] = withoutSavepoint.toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun getManualAction(flinkCluster: V1FlinkCluster): String {
        return flinkCluster.metadata?.annotations?.get(MANUAL_ACTION)?.toUpperCase() ?: "NONE"
    }

    fun isWithSavepoint(flinkCluster: V1FlinkCluster): Boolean {
        return flinkCluster.metadata?.annotations?.get(WITHOUT_SAVEPOINT)?.toUpperCase() == "TRUE"
    }

    val FLINK_OPERATOR_TIMESTAMP            = "nextbreakpoint.com/flink-operator-timestamp"
    val FLINK_OPERATOR_TASKS                = "nextbreakpoint.com/flink-operator-tasks"
    val FLINK_OPERATOR_TASK_STATUS          = "nextbreakpoint.com/flink-operator-task-status"
    val FLINK_OPERATOR_TASK_ATTEMPTS        = "nextbreakpoint.com/flink-operator-task-attempts"
    val FLINK_OPERATOR_CLUSTER_STATUS       = "nextbreakpoint.com/flink-operator-cluster-status"
    val FLINK_OPERATOR_SAVEPOINT_PATH       = "nextbreakpoint.com/flink-operator-savepoint-path"
    val FLINK_OPERATOR_SAVEPOINT_REQUEST    = "nextbreakpoint.com/flink-operator-savepoint-request"
    val FLINK_OPERATOR_SAVEPOINT_TIMESTAMP  = "nextbreakpoint.com/flink-operator-savepoint-timestamp"
    val FLINK_OPERATOR_JOBMANAGER_DIGEST    = "nextbreakpoint.com/flink-operator-digest-jobmanager"
    val FLINK_OPERATOR_TASKMANAGER_DIGEST   = "nextbreakpoint.com/flink-operator-digest-taskmanager"
    val FLINK_OPERATOR_IMAGE_DIGEST         = "nextbreakpoint.com/flink-operator-digest-image"
    val FLINK_OPERATOR_JOB_DIGEST           = "nextbreakpoint.com/flink-operator-digest-job"

    fun hasCurrentTask(flinkCluster: V1FlinkCluster) : Boolean =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS) != null

    fun getCurrentTask(flinkCluster: V1FlinkCluster) : OperatorTask =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS)?.split(" ")
            ?.filter { it.isNotBlank() }?.map { OperatorTask.valueOf(it) }?.firstOrNull() ?: OperatorTask.CLUSTER_HALTED

    fun getCurrentTaskStatus(flinkCluster: V1FlinkCluster) : TaskStatus {
        val status = flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASK_STATUS)
        return if (status.isNullOrBlank()) TaskStatus.EXECUTING else TaskStatus.valueOf(status)
    }

    fun getClusterStatus(flinkCluster: V1FlinkCluster) : ClusterStatus {
        val status = flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_CLUSTER_STATUS)
        return if (status.isNullOrBlank()) ClusterStatus.UNKNOWN else ClusterStatus.valueOf(status)
    }

    fun getOperatorTimestamp(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TIMESTAMP)?.toLong() ?: 0

    fun getSavepointPath(flinkCluster: V1FlinkCluster) : String? =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_SAVEPOINT_PATH)

    fun getSavepointRequest(flinkCluster: V1FlinkCluster) : SavepointRequest? {
        val request = flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_SAVEPOINT_REQUEST)
        return if (request != null && request != "") Gson().fromJson(request, SavepointRequest::class.java) else null
    }

    fun getSavepointTimestamp(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_SAVEPOINT_TIMESTAMP)?.toLong() ?: 0

    fun getNextOperatorTask(flinkCluster: V1FlinkCluster) : OperatorTask? =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS)?.split(' ')?.drop(1)?.map { OperatorTask.valueOf(it) }?.firstOrNull()

    fun selectNextTask(flinkCluster: V1FlinkCluster) {
        ensureAnnotations(flinkCluster)

        val task = flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS)?.split(' ')?.firstOrNull() ?: OperatorTask.CLUSTER_HALTED.toString()

        val tasks = flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS)?.split(' ')?.drop(1)?.joinToString(separator = " ") ?: ""

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKS] = tasks.ifBlank { task }

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun appendTasks(flinkCluster: V1FlinkCluster, tasks: List<OperatorTask>) {
        ensureAnnotations(flinkCluster)

        val currentTask = flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS) ?: ""

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKS] = (currentTask + " " + tasks.joinToString(separator = " ")).trim()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun resetTasks(flinkCluster: V1FlinkCluster, tasks: List<OperatorTask>) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKS] = tasks.joinToString(separator = " ")

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun setTaskStatus(flinkCluster: V1FlinkCluster, status: TaskStatus) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASK_STATUS] = status.toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun setSavepointPath(flinkCluster: V1FlinkCluster, path: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_TIMESTAMP] = currentTimeMillis().toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_PATH] = path

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun setSavepointRequest(flinkCluster: V1FlinkCluster, request: SavepointRequest) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_TIMESTAMP] = currentTimeMillis().toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_REQUEST] = Gson().toJson(request)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun updateSavepointTimestamp(flinkCluster: V1FlinkCluster) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_TIMESTAMP] = currentTimeMillis().toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_REQUEST] = ""

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun setClusterStatus(flinkCluster: V1FlinkCluster, status: ClusterStatus) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_CLUSTER_STATUS] = status.toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun setJobManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_JOBMANAGER_DIGEST] = digest

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun setTaskManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKMANAGER_DIGEST] = digest

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun setFlinkImageDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_IMAGE_DIGEST] = digest

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun setFlinkJobDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_JOB_DIGEST] = digest

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun getJobManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.metadata.annotations[FLINK_OPERATOR_JOBMANAGER_DIGEST]

    fun getTaskManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKMANAGER_DIGEST]

    fun getFlinkImageDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.metadata.annotations[FLINK_OPERATOR_IMAGE_DIGEST]

    fun getFlinkJobDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.metadata.annotations[FLINK_OPERATOR_JOB_DIGEST]

    fun setOperatorTaskAttempts(flinkCluster: V1FlinkCluster, attempts: Int) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASK_ATTEMPTS] = attempts.toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = currentTimeMillis().toString()
    }

    fun getOperatorTaskAttempts(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASK_ATTEMPTS]?.toInt() ?: 0

    private fun currentTimeMillis(): Long {
        try {
            Thread.sleep(1)
        } catch (e : Exception) {
        }
        return System.currentTimeMillis()
    }

    private fun ensureAnnotations(flinkCluster: V1FlinkCluster) {
        if (flinkCluster.metadata.annotations == null) {
            flinkCluster.metadata.annotations = mutableMapOf()
        }
    }
}