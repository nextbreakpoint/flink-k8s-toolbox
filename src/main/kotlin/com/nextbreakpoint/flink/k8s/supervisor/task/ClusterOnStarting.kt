package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.k8s.supervisor.core.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager

class ClusterOnStarting : Task<ClusterManager>() {
    private val actions = setOf(
        ManualAction.STOP
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

        val resourcesExist = manager.ensureJobManagerPodExists() && manager.ensureJobManagerServiceExists()

        if (!resourcesExist) {
            return
        }

        if (!manager.isClusterHealthy()) {
            return
        }

        if (!manager.stopAllJobs()) {
            return
        }

        if (!manager.createJobs()) {
            return
        }

        manager.onClusterStarted()
    }
}