package com.nextbreakpoint.common.model

import com.nextbreakpoint.operator.OperatorContext

interface TaskHandler {
    fun onExecuting(context: OperatorContext): Result<String>

    fun onAwaiting(context: OperatorContext): Result<String>

    fun onIdle(context: OperatorContext)

    fun onFailed(context: OperatorContext)
}