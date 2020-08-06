package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import org.joda.time.DateTime

object FlinkClusterAnnotations {
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

    fun getActionTimestamp(flinkCluster: V2FlinkCluster) : DateTime =
        DateTime(flinkCluster.metadata?.annotations?.get(ACTION_TIMESTAMP)?.toLong() ?: 0L)

    fun setManualAction(flinkCluster: V2FlinkCluster, manualAction: ManualAction) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[MANUAL_ACTION] = manualAction.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun getManualAction(flinkCluster: V2FlinkCluster): ManualAction {
        return ManualAction.valueOf(flinkCluster.metadata?.annotations?.get(MANUAL_ACTION)?.toUpperCase() ?: "NONE")
    }

    fun setWithoutSavepoint(flinkCluster: V2FlinkCluster, withoutSavepoint: Boolean) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[WITHOUT_SAVEPOINT] = withoutSavepoint.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun isWithoutSavepoint(flinkCluster: V2FlinkCluster): Boolean {
        return flinkCluster.metadata?.annotations?.get(WITHOUT_SAVEPOINT)?.toUpperCase() == "TRUE"
    }

    fun setDeleteResources(flinkCluster: V2FlinkCluster, deleteResources: Boolean) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[DELETE_RESOURCES] = deleteResources.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun isDeleteResources(flinkCluster: V2FlinkCluster): Boolean {
        return flinkCluster.metadata?.annotations?.get(DELETE_RESOURCES)?.toUpperCase() == "TRUE"
    }

    fun setShouldRestart(flinkCluster: V2FlinkCluster, shouldRestart: Boolean) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[SHOULD_RESTART] = shouldRestart.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun shouldRestart(flinkCluster: V2FlinkCluster): Boolean {
        return flinkCluster.metadata?.annotations?.get(SHOULD_RESTART)?.toUpperCase() == "TRUE"
    }
}