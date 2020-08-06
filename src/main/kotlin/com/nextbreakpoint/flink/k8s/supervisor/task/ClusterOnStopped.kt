package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.k8s.supervisor.core.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager

class ClusterOnStopped : Task<ClusterManager>() {
    private val actions = setOf(
        ManualAction.START
    )

    override fun execute(manager: ClusterManager) {
        if (manager.isResourceDeleted()) {
            manager.onResourceDeleted()
            return
        }

        if (manager.isActionPresent()) {
            manager.executeAction(actions)
            return
        }

        manager.setClusterHealth("")

        if (manager.stopCluster()) {
            manager.setResourceUpdated(true)
        } else {
            manager.setResourceUpdated(false)
        }
    }
}