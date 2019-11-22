package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

class EraseSavepoint : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        Status.setSavepointPath(context.flinkCluster, null)

        return taskCompletedWithOutput(context.flinkCluster, "Erasing savepoint...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        return taskCompletedWithOutput(context.flinkCluster, "Savepoint erased")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "Nothing to do")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}