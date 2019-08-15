package com.nextbreakpoint.flinkoperator.cli

import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig

abstract class RemoteCommand<T>(val factory: WebClientFactory) {
    abstract fun run(connectionConfig: ConnectionConfig, clusterName: String, args: T)
}

