package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.common.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager

class JobOnStarting : Task<JobManager>() {
    private val actions = setOf(
        Action.STOP
    )

    override fun execute(manager: JobManager) {
        if (manager.isResourceDeleted()) {
            manager.onResourceDeleted()
            return
        }

        if (manager.isClusterTerminated()) {
            manager.onClusterStopping()
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

        val clusterUpdated = manager.isClusterUpdated()

        if (clusterUpdated) {
            manager.setResourceUpdated(true)
        } else {
            manager.setResourceUpdated(false)
        }

        if (!clusterUpdated) {
            return
        }

        if (manager.isRestartTimeout()) {
            manager.onJobAborted()
            return
        }

        if (manager.mustResetSavepoint()) {
            manager.resetSavepoint()
            return
        }

        if (manager.isJobSuspended()) {
            manager.onJobStopped()
            return
        }

        if (!manager.ensureBootstrapJobExists()) {
            return
        }

        if (manager.isJobStarted()) {
            manager.onJobStarted()
            return
        }
    }
}