package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.Result

interface Task {
    fun onExecuting(context: TaskContext): Result<String>

    fun onAwaiting(context: TaskContext): Result<String>

    fun onIdle(context: TaskContext): Result<String>

    fun onFailed(context: TaskContext): Result<String>
}