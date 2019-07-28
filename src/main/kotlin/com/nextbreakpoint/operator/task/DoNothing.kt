package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorContext

class DoNothing : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        return Result(ResultStatus.SUCCESS, "There is nothing to do")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(ResultStatus.SUCCESS, "I'll wait for the next task")
    }

    override fun onIdle(context: OperatorContext) {
    }

    override fun onFailed(context: OperatorContext) {
    }
}