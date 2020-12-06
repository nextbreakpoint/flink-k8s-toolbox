package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.common.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager

class JobOnTerminated : Task<JobManager>() {
    override fun execute(manager: JobManager) {
        if (manager.hasFinalizer()) {
            manager.removeFinalizer()
        }
    }
}