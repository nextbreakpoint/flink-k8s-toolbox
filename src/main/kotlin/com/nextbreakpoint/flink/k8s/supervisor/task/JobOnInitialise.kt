package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.common.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager

class JobOnInitialise : Task<JobManager>() {
    override fun execute(manager: JobManager) {
        if (manager.isClusterTerminated()) {
            return
        }

        if (!manager.hasFinalizer()) {
            if (manager.isResourceDeleted()) {
                manager.onJobTerminated()
            } else {
                manager.addFinalizer()
            }
        } else {
            manager.onResourceInitialise()
        }
    }
}