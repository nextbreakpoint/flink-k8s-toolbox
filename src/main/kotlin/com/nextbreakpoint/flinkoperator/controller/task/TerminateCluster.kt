package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult

class TerminateCluster : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Terminated)
        Status.setTaskAttempts(context.flinkCluster, 0)

        return skip(context.flinkCluster, "Cluster terminated")
    }
}