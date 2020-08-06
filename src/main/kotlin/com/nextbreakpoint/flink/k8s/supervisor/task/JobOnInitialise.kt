package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.supervisor.core.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager

class JobOnInitialise : Task<JobManager>() {
    override fun execute(manager: JobManager) {
        manager.onResourceInitialise()
    }
}