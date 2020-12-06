package com.nextbreakpoint.flink.cli.core

import com.nextbreakpoint.flink.common.ConnectionConfig

interface JobCommand<T> {
    fun run(connectionConfig: ConnectionConfig, clusterName: String, jobName: String, args: T)
}

