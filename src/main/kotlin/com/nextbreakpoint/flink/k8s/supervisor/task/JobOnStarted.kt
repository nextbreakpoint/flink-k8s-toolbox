package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.k8s.supervisor.core.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager

class JobOnStarted : Task<JobManager>() {
    private val actions = setOf(
        ManualAction.START,
        ManualAction.STOP,
        ManualAction.FORGET_SAVEPOINT,
        ManualAction.TRIGGER_SAVEPOINT
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

        if (manager.isClusterUnhealthy()) {
            manager.setClusterHealth("UNHEALTHY")
            manager.onClusterUnhealthy()
            return
        }

        if (manager.hasSpecificationChanged()) {
            manager.onResourceChanged()
            return
        }

        if (manager.hasParallelismChanged()) {
            manager.onResourceChanged()
            return
        }

        if (manager.isJobFinished()) {
            manager.onJobFinished()
            return
        }

        if (manager.isJobCancelled()) {
            manager.onJobCanceled()
            return
        }

        if (manager.isJobFailed()) {
            manager.onJobFailed()
            return
        }

        if (manager.isActionPresent()) {
            manager.executeAction(actions)
            return
        }

        if (manager.updateSavepoint()) {
            manager.setResourceUpdated(true)
        } else {
            manager.setResourceUpdated(false)
        }

        manager.updateJobStatus()
    }
}