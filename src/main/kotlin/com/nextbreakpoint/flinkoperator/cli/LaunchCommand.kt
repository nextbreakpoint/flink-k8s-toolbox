package com.nextbreakpoint.flinkoperator.cli

import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions

interface LaunchCommand<T> {
    fun run(flinkOptions: FlinkOptions, namespace: String, args: T)
}

