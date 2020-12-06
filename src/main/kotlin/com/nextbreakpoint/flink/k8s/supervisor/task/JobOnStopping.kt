package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.common.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager

class JobOnStopping : Task<JobManager>() {
    override fun execute(manager: JobManager) {
        if (!manager.terminateBootstrapJob()) {
            return
        }

        if (manager.isClusterStarted()) {
            manager.setClusterHealth("HEALTHY")
        } else {
            manager.setClusterHealth("")
        }

        if (manager.isClusterUnhealthy()) {
            manager.setClusterHealth("UNHEALTHY")
            if (manager.mustTerminateResources()) {
                manager.onJobTerminated()
            } else {
                manager.onJobStopped()
            }
            return
        }

        if (!manager.cancelJob()) {
            return
        }

        if (manager.mustTerminateResources()) {
            manager.onJobTerminated()
        } else {
            if (manager.shouldRestart()) {
                manager.onJobReadyToRestart()
            } else {
                manager.onJobStopped()
            }
        }
    }
}