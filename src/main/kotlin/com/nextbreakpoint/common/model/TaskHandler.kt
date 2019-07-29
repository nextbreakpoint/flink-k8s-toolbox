package com.nextbreakpoint.common.model

import com.nextbreakpoint.operator.OperatorContext

interface TaskHandler {
    fun onExecuting(context: OperatorContext): Result<String>

    fun onAwaiting(context: OperatorContext): Result<String>

    fun onIdle(context: OperatorContext): Result<String>

    fun onFailed(context: OperatorContext): Result<String>
}