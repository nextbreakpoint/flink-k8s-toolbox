package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.model.Result

interface OperatorTaskHandler {
    fun onExecuting(context: OperatorContext): Result<String>

    fun onAwaiting(context: OperatorContext): Result<String>

    fun onIdle(context: OperatorContext): Result<String>

    fun onFailed(context: OperatorContext): Result<String>
}