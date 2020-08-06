package com.nextbreakpoint.flink.cli.core

import com.nextbreakpoint.flink.common.FlinkOptions

interface LaunchCommand<T> {
    fun run(flinkOptions: FlinkOptions, namespace: String, args: T)
}

