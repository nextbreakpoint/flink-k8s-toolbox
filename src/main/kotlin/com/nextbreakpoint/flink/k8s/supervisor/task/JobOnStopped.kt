package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.k8s.supervisor.core.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager

class JobOnStopped : Task<JobManager>() {
    private val actions = setOf(
        ManualAction.START
    )

    override fun execute(manager: JobManager) {
        if (manager.isResourceDeleted()) {
            manager.onResourceDeleted()
            return
        }

        if (!manager.terminateBootstrapJob()) {
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