package com.nextbreakpoint.flinkoperator.cli

import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig

abstract class OperatorCommand(val factory: WebClientFactory) {
    abstract fun run(connectionConfig: ConnectionConfig)
}



