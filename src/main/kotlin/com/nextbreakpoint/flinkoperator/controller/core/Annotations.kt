package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import org.joda.time.DateTime

object Annotations {
    val MANUAL_ACTION       = "flink-operator/manual-action"
    val ACTION_TIMESTAMP    = "flink-operator/action-timestamp"
    val WITHOUT_SAVEPOINT   = "flink-operator/without-savepoint"
    val DELETE_RESOURCES    = "flink-operator/delete-resources"

    private fun currentTimeMillis(): Long {
        try {
            // this is a hack for testing
            Thread.sleep(1)
        } catch (e : Exception) {}
        return System.currentTimeMillis()
    }

    fun getActionTimestamp(flinkCluster: V1FlinkCluster) : DateTime =
        DateTime(flinkCluster.metadata?.annotations?.get(ACTION_TIMESTAMP)?.toLong() ?: 0L)

    fun setManualAction(flinkCluster: V1FlinkCluster, manualAction: ManualAction) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[MANUAL_ACTION] = manualAction.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun getManualAction(flinkCluster: V1FlinkCluster): ManualAction {
        return ManualAction.valueOf(flinkCluster.metadata?.annotations?.get(MANUAL_ACTION)?.toUpperCase() ?: "NONE")
    }

    fun setWithoutSavepoint(flinkCluster: V1FlinkCluster, withoutSavepoint: Boolean) {
        val annotations = flinkCluster.metadata?.annotations.orEmpty().toMutableMap()
        annotations[WITHOUT_SAVEPOINT] = withoutSavepoint.toString()
        annotations[ACTION_TIMESTAMP] = currentTimeMillis().toString()
        flinkCluster.metadata?.annotations = annotations
    }

    fun isWithSavepoint(flinkCluster: V1FlinkCluster): Boolean {
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
}