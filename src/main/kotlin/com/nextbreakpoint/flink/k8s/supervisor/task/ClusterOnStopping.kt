package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.common.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager

class ClusterOnStopping : Task<ClusterManager>() {
    private val actions = setOf(
        Action.START
    )

    override fun execute(manager: ClusterManager) {
        if (!manager.waitForJobs()) {
            return
        }

        manager.setClusterHealth("")

        if (!manager.stopCluster()) {
            return
        }

        if (manager.mustTerminateResources()) {
            manager.onClusterTerminated()
            return
        }

        if (manager.shouldRestart()) {
            manager.onClusterReadyToRestart()
            return
        }

        if (manager.isActionPresent()) {
            manager.executeAction(actions)
            return
        }

        manager.onClusterStopped()
    }
}