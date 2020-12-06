package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import org.joda.time.DateTime

object FlinkClusterAnnotations {
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

    fun getActionTimestamp(flinkCluster: V1FlinkCluster) : DateTime =
        DateTime(flinkCluster.metadata?.annotations?.get(ACTION_TIMESTAMP)?.toLong() ?: 0L)

    fun setRequestedAction(flinkCluster: V1FlinkCluster, action: Action) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[REQUESTED_ACTION] = action.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun getRequestedAction(flinkCluster: V1FlinkCluster): Action {
        return Action.valueOf(flinkCluster.metadata?.annotations?.get(REQUESTED_ACTION)?.toUpperCase() ?: "NONE")
    }

    fun setWithoutSavepoint(flinkCluster: V1FlinkCluster, withoutSavepoint: Boolean) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[WITHOUT_SAVEPOINT] = withoutSavepoint.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun isWithoutSavepoint(flinkCluster: V1FlinkCluster): Boolean {
        return flinkCluster.metadata?.annotations?.get(WITHOUT_SAVEPOINT)?.toUpperCase() == "TRUE"
    }

    fun setDeleteResources(flinkCluster: V1FlinkCluster, deleteResources: Boolean) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[DELETE_RESOURCES] = deleteResources.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun isDeleteResources(flinkCluster: V1FlinkCluster): Boolean {
        return flinkCluster.metadata?.annotations?.get(DELETE_RESOURCES)?.toUpperCase() == "TRUE"
    }

    fun setShouldRestart(flinkCluster: V1FlinkCluster, shouldRestart: Boolean) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[SHOULD_RESTART] = shouldRestart.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun shouldRestart(flinkCluster: V1FlinkCluster): Boolean {
        return flinkCluster.metadata?.annotations?.get(SHOULD_RESTART)?.toUpperCase() == "TRUE"
    }
}