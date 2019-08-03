package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.ConnectionConfig

abstract class CommandNoArgs(val factory: WebClientFactory) {
    abstract fun run(connectionConfig: ConnectionConfig, clusterName: String)
}



