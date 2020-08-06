package com.nextbreakpoint.flink.cli.core

import com.nextbreakpoint.flink.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.common.ConnectionConfig

abstract class JobCommand<T>(val factory: WebClientFactory) {
    abstract fun run(connectionConfig: ConnectionConfig, clusterName: String, jobName: String, args: T)
}

