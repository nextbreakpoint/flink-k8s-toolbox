package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import org.joda.time.DateTime

object FlinkJobAnnotations {
    val REQUESTED_ACTION    = "operator.nextbreakpoint.com/requested-action"
    val ACTION_TIMESTAMP    = "operator.nextbreakpoint.com/action-timestamp"
    val WITHOUT_SAVEPOINT   = "operator.nextbreakpoint.com/without-savepoint"
    val DELETE_RESOURCES    = "operator.nextbreakpoint.com/delete-resources"
    val SHOULD_RESTART      = "operator.nextbreakpoint.com/should-restart"

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

    fun getActionTimestamp(flinkJob: V1FlinkJob) : DateTime =
        DateTime(flinkJob.metadata?.annotations?.get(ACTION_TIMESTAMP)?.toLong() ?: 0L)

    fun setRequestedAction(flinkJob: V1FlinkJob, action: Action) {
        val annotations = flinkJob.metadata?.annotations.orEmpty().toMutableMap()
        annotations[REQUESTED_ACTION] = action.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkJob.metadata?.annotations = annotations
    }

    fun getRequestedAction(flinkJob: V1FlinkJob): Action {
        return Action.valueOf(flinkJob.metadata?.annotations?.get(REQUESTED_ACTION)?.toUpperCase() ?: "NONE")
    }

    fun setWithoutSavepoint(flinkJob: V1FlinkJob, withoutSavepoint: Boolean) {
        val annotations = flinkJob.metadata?.annotations.orEmpty().toMutableMap()
        annotations[WITHOUT_SAVEPOINT] = withoutSavepoint.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkJob.metadata?.annotations = annotations
    }

    fun isWithoutSavepoint(flinkJob: V1FlinkJob): Boolean {
        return flinkJob.metadata?.annotations?.get(WITHOUT_SAVEPOINT)?.toUpperCase() == "TRUE"
    }

    fun setDeleteResources(flinkJob: V1FlinkJob, deleteResources: Boolean) {
        val annotations = flinkJob.metadata?.annotations.orEmpty().toMutableMap()
        annotations[DELETE_RESOURCES] = deleteResources.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkJob.metadata?.annotations = annotations
    }

    fun isDeleteResources(flinkJob: V1FlinkJob): Boolean {
        return flinkJob.metadata?.annotations?.get(DELETE_RESOURCES)?.toUpperCase() == "TRUE"
    }

    fun setShouldRestart(flinkJob: V1FlinkJob, shouldRestart: Boolean) {
        val annotations = flinkJob.metadata?.annotations.orEmpty().toMutableMap()
        annotations[SHOULD_RESTART] = shouldRestart.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkJob.metadata?.annotations = annotations
    }

    fun shouldRestart(flinkJob: V1FlinkJob): Boolean {
        return flinkJob.metadata?.annotations?.get(SHOULD_RESTART)?.toUpperCase() == "TRUE"
    }
}