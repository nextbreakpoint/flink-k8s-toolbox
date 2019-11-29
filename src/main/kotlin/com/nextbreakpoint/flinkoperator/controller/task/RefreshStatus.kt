package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

class RefreshStatus : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        updateDigests(context.flinkCluster)
        updateBootstrap(context.flinkCluster)

        return skip(context.flinkCluster, "Status refreshed")
    }
}