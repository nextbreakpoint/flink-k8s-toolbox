package com.nextbreakpoint.flinkoperator.client.core

import com.nextbreakpoint.flinkoperator.client.factory.WebClientFactory
import com.nextbreakpoint.flinkoperator.common.ConnectionConfig

abstract class ClusterCommand<T>(val factory: WebClientFactory) {
    abstract fun run(connectionConfig: ConnectionConfig, clusterName: String, args: T)
}

