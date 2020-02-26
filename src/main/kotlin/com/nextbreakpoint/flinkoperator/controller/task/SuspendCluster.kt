package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult

class SuspendCluster : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Suspended)
        Status.setTaskAttempts(context.flinkCluster, 0)

        return skip(context.flinkCluster, "Cluster suspended")
    }
}