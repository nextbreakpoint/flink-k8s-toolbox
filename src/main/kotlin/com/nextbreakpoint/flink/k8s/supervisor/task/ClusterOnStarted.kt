package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.common.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager

class ClusterOnStarted : Task<ClusterManager>() {
    private val actions = setOf(
        Action.START,
        Action.STOP
    )

    override fun execute(manager: ClusterManager) {
        if (manager.isResourceDeleted()) {
            manager.onResourceDeleted()
            return
        }

        if (manager.hasResourceDiverged()) {
            manager.onResourceDiverged()
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

        if (manager.isClusterUnhealthy()) {
            manager.setClusterHealth("UNHEALTHY")
            manager.onClusterUnhealthy()
            return
        }

        manager.setClusterHealth("HEALTHY")

        if (manager.rescaleTaskManagers()) {
            manager.setResourceUpdated(false)
            return
        }

        if (manager.rescaleTaskManagerPods()) {
            manager.setResourceUpdated(false)
            return
        }

        if (!manager.isClusterReady()) {
            manager.setResourceUpdated(false)
            return
        }

        manager.setResourceUpdated(true)

        if (!manager.areJobsReady()) {
            return
        }

        manager.stopUnmanagedJobs()
    }
}