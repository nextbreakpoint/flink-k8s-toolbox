package com.nextbreakpoint.flink.k8s.supervisor.task

import com.nextbreakpoint.flink.k8s.supervisor.core.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager

class ClusterOnInitialize : Task<ClusterManager>() {
    override fun execute(manager: ClusterManager) {
        manager.onResourceInitialise()
    }
}