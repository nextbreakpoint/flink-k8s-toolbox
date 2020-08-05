package com.nextbreakpoint.flinkoperator.client.core

import com.nextbreakpoint.flinkoperator.client.factory.WebClientFactory
import com.nextbreakpoint.flinkoperator.common.ConnectionConfig

abstract class OperatorCommand(val factory: WebClientFactory) {
    abstract fun run(connectionConfig: ConnectionConfig)
}



