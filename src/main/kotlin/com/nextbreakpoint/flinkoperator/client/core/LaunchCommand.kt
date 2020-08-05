package com.nextbreakpoint.flinkoperator.client.core

import com.nextbreakpoint.flinkoperator.common.FlinkOptions

interface LaunchCommand<T> {
    fun run(flinkOptions: FlinkOptions, namespace: String, args: T)
}

