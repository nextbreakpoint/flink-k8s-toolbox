package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import org.joda.time.DateTime

object FlinkJobAnnotations {
    val MANUAL_ACTION       = "flink-operator/manual-action"
    val ACTION_TIMESTAMP    = "flink-operator/action-timestamp"
    val WITHOUT_SAVEPOINT   = "flink-operator/without-savepoint"
    val DELETE_RESOURCES    = "flink-operator/delete-resources"
    val SHOULD_RESTART      = "flink-operator/should-restart"

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

    fun setManualAction(flinkJob: V1FlinkJob, manualAction: ManualAction) {
        val annotations = flinkJob.metadata?.annotations.orEmpty().toMutableMap()
        annotations[MANUAL_ACTION] = manualAction.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkJob.metadata?.annotations = annotations
    }

    fun getManualAction(flinkJob: V1FlinkJob): ManualAction {
        return ManualAction.valueOf(flinkJob.metadata?.annotations?.get(MANUAL_ACTION)?.toUpperCase() ?: "NONE")
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