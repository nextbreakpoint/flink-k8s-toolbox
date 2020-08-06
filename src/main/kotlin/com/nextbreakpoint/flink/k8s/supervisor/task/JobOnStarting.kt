package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.k8s.supervisor.core.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager

class JobOnStarting : Task<JobManager>() {
    private val actions = setOf(
        ManualAction.STOP
    )

    override fun execute(manager: JobManager) {
        if (manager.isResourceDeleted()) {
            manager.onResourceDeleted()
            return
        }

        if (manager.isClusterStopping()) {
            manager.onClusterStopping()
            return
        }

        if (manager.isClusterStopped()) {
            manager.onJobAborted()
            return
        }

        if (manager.isClusterStarting()) {
            manager.onJobAborted()
            return
        }

        if (manager.isClusterStarted()) {
            manager.setClusterHealth("HEALTHY")
        } else {
            manager.setClusterHealth("")
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

        if (manager.isClusterUpdated()) {
            manager.setResourceUpdated(true)
        } else {
            manager.setResourceUpdated(false)
        }

        if (manager.hasTaskTimedOut()) {
            manager.onJobAborted()
            return
        }

        if (manager.startJob()) {
            manager.onJobStarted()
            return
        }
    }
}