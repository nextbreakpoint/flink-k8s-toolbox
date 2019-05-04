package com.nextbreakpoint.operator

import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result

abstract class OperatorCommand<T, R>(val flinkOptions : FlinkOptions) {
    abstract fun execute(clusterId: ClusterId, params: T): Result<R>
}