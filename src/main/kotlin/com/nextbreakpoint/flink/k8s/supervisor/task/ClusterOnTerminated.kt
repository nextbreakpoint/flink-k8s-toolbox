package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.common.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager

class ClusterOnTerminated : Task<ClusterManager>() {
    override fun execute(manager: ClusterManager) {
        if (manager.hasFinalizer()) {
            manager.removeFinalizer()
        }
    }
}