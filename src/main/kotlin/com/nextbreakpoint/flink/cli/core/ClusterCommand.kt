package com.nextbreakpoint.flink.cli.core

import com.nextbreakpoint.flink.common.ConnectionConfig

interface ClusterCommand<T> {
    fun run(connectionConfig: ConnectionConfig, clusterName: String, args: T)
}

