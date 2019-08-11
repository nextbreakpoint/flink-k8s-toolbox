package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result

abstract class OperatorCommand<T, R>(val flinkOptions : FlinkOptions) {
    abstract fun execute(clusterId: ClusterId, params: T): Result<R>
}