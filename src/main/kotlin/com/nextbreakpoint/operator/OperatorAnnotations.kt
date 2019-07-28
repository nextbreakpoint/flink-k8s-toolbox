package com.nextbreakpoint.operator

import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.OperatorTask
import com.nextbreakpoint.common.model.TaskStatus
import com.nextbreakpoint.model.V1FlinkCluster

object OperatorAnnotations {
    val FLINK_OPERATOR_TIMESTAMP            = "flink-operator-timestamp"
    val FLINK_OPERATOR_TASKS                = "flink-operator-tasks"
    val FLINK_OPERATOR_TASK_STATUS          = "flink-operator-task-status"
    val FLINK_OPERATOR_CLUSTER_STATUS       = "flink-operator-cluster-status"
    val FLINK_OPERATOR_SAVEPOINT_PATH       = "flink-operator-savepoint-path"
    val FLINK_OPERATOR_SAVEPOINT_REQUEST    = "flink-operator-savepoint-request"
    val FLINK_OPERATOR_SAVEPOINT_TIESTAMP   = "flink-operator-savepoint-timestamp"
    val FLINK_OPERATOR_CLUSTER_DIGEST       = "flink-operator-digest-cluster"
    val FLINK_OPERATOR_JOBMANAGER_DIGEST    = "flink-operator-digest-jobmanager"
    val FLINK_OPERATOR_TASKMANAGER_DIGEST   = "flink-operator-digest-taskmanager"
    val FLINK_OPERATOR_IMAGE_DIGEST         = "flink-operator-digest-image"
    val FLINK_OPERATOR_JOB_DIGEST           = "flink-operator-digest-job"

    fun hasCurrentOperatorTask(flinkCluster: V1FlinkCluster) : Boolean =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS) != null

    fun getCurrentOperatorTask(flinkCluster: V1FlinkCluster) : OperatorTask =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS)?.split(" ")?.filter { it.isNotBlank() }?.map { OperatorTask.valueOf(it) }?.firstOrNull() ?: OperatorTask.DO_NOTHING

    fun getCurrentOperatorTasks(flinkCluster: V1FlinkCluster): List<OperatorTask> =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS)?.split(" ")?.filter { it.isNotBlank() }?.map { OperatorTask.valueOf(it) }?.toList() ?: listOf(OperatorTask.DO_NOTHING)

    fun getCurrentOperatorStatus(flinkCluster: V1FlinkCluster) : TaskStatus {
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
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_SAVEPOINT_PATH)?.trim('\"')

    fun getSavepointRequest(flinkCluster: V1FlinkCluster) : String? =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_SAVEPOINT_REQUEST)

    fun getSavepointTimestamp(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_SAVEPOINT_TIESTAMP)?.toLong() ?: 0

    fun getNextOperatorTask(flinkCluster: V1FlinkCluster) : OperatorTask? =
        flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS)?.split(' ')?.drop(1)?.map { OperatorTask.valueOf(it) }?.firstOrNull()

    fun appendOperatorTask(flinkCluster: V1FlinkCluster, task: OperatorTask) {
        ensureAnnotations(flinkCluster)

        val tasks = flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKS] ?: ""

        val newTasks = tasks.split(" ") + task.toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKS] = newTasks.joinToString(separator = " ")

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = System.currentTimeMillis().toString()
    }

    fun advanceOperatorTask(flinkCluster: V1FlinkCluster) {
        ensureAnnotations(flinkCluster)

        val task = flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS)?.split(' ')?.firstOrNull() ?: "DO_NOTHING"

        val tasks = flinkCluster.metadata?.annotations?.get(FLINK_OPERATOR_TASKS)?.split(' ')?.drop(1)?.joinToString(separator = " ") ?: ""

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKS] = tasks.ifBlank { task }

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = System.currentTimeMillis().toString()
    }

    fun resetOperatorTasks(flinkCluster: V1FlinkCluster, task: List<OperatorTask>) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKS] = task.joinToString(separator = " ")

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = System.currentTimeMillis().toString()
    }

    fun setOperatorStatus(flinkCluster: V1FlinkCluster, status: TaskStatus) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASK_STATUS] = status.toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TIMESTAMP] = System.currentTimeMillis().toString()
    }

    fun setSavepointRequest(flinkCluster: V1FlinkCluster, requests: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_TIESTAMP] = System.currentTimeMillis().toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_REQUEST] = requests
    }

    fun setSavepointPath(flinkCluster: V1FlinkCluster, savepointPath: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_TIESTAMP] = System.currentTimeMillis().toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_PATH] = savepointPath

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_REQUEST] = "{}"
    }

    fun updateSavepointTimestamp(flinkCluster: V1FlinkCluster) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_TIESTAMP] = System.currentTimeMillis().toString()

        flinkCluster.metadata.annotations[FLINK_OPERATOR_SAVEPOINT_REQUEST] = "{}"
    }

    fun setClusterStatus(flinkCluster: V1FlinkCluster, status: ClusterStatus) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_CLUSTER_STATUS] = status.toString()
    }

    fun setFlinkClusterDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_CLUSTER_DIGEST] = digest
    }

    fun setJobManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_JOBMANAGER_DIGEST] = digest
    }

    fun setTaskManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKMANAGER_DIGEST] = digest
    }

    fun setFlinkImageDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_IMAGE_DIGEST] = digest
    }

    fun setFlinkJobDigest(flinkCluster: V1FlinkCluster, digest: String) {
        ensureAnnotations(flinkCluster)

        flinkCluster.metadata.annotations[FLINK_OPERATOR_JOB_DIGEST] = digest
    }

    fun getFlinkClusterDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.metadata.annotations[FLINK_OPERATOR_CLUSTER_DIGEST]

    fun getJobManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.metadata.annotations[FLINK_OPERATOR_JOBMANAGER_DIGEST]

    fun getTaskManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.metadata.annotations[FLINK_OPERATOR_TASKMANAGER_DIGEST]

    fun getFlinkImageDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.metadata.annotations[FLINK_OPERATOR_IMAGE_DIGEST]

    fun getFlinkJobDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.metadata.annotations[FLINK_OPERATOR_JOB_DIGEST]

    private fun ensureAnnotations(flinkCluster: V1FlinkCluster) {
        if (flinkCluster.metadata.annotations == null) {
            flinkCluster.metadata.annotations = mutableMapOf()
        }
    }
}