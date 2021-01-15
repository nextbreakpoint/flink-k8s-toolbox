package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.common.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager

class ClusterOnStarting : Task<ClusterManager>() {
    private val actions = setOf(
        Action.STOP
    )

    override fun execute(manager: ClusterManager) {
        if (manager.isResourceDeleted()) {
            manager.onResourceDeleted()
            return
        }

        if (manager.hasSpecificationChanged()) {
            manager.onResourceChanged()
            return
        }

        if (manager.isActionPresent()) {
            manager.executeAction(actions)
            return
        }

        manager.setClusterHealth("")

        if (!manager.ensureJobManagerPodExists()) {
            return
        }

        if (!manager.ensureJobManagerServiceExists()) {
            return
        }

        if (!manager.isClusterHealthy()) {
            return
        }

        if (!manager.stopAllJobs()) {
            return
        }

        manager.onClusterStarted()
    }
}