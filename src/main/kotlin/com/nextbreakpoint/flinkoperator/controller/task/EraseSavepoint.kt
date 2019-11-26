package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

class EraseSavepoint : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        Status.setSavepointPath(context.flinkCluster, "")

        return skip(context.flinkCluster, "Savepoint erased from status")
    }
}