package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.common.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager

class JobOnStopped : Task<JobManager>() {
    private val actions = setOf(
        Action.START
    )

    override fun execute(manager: JobManager) {
        if (manager.isResourceDeleted()) {
            manager.onResourceDeleted()
            return
        }

        if (!manager.terminateBootstrapJob()) {
            return
        }

        if (manager.isClusterStarting()) {
            if (manager.shouldRestartJob()) {
                manager.onJobReadyToRestart()
            }
            return
        }

        if (!manager.isClusterStarted()) {
            manager.setClusterHealth("")
            return
        }

        if (manager.isClusterUnhealthy()) {
            manager.setClusterHealth("UNHEALTHY")
            return
        }

        manager.setClusterHealth("HEALTHY")

        if (manager.isActionPresent()) {
            manager.executeAction(actions)
            return
        }

        if (!manager.shouldRestartJob()) {
            return
        }

        if (manager.hasSpecificationChanged()) {
            manager.onResourceChanged()
            return
        }

        if (!manager.hasTaskTimedOut()) {
            return
        }

        manager.onJobReadyToRestart()
    }
}