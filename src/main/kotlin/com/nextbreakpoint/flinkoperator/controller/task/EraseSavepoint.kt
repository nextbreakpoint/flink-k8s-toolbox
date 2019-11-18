package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task

class EraseSavepoint : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        Status.setSavepointPath(context.flinkCluster, null)

        return Result(
            ResultStatus.SUCCESS,
            "Erasing savepoint of cluster ${context.clusterId.name}..."
        )
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        return Result(
            ResultStatus.SUCCESS,
            "Savepoint of cluster ${context.clusterId.name} erased"
        )
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }
}