package com.nextbreakpoint.flinkoperator.cli

import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig

abstract class OperatorCommandNoArgs(val factory: WebClientFactory) {
    abstract fun run(connectionConfig: ConnectionConfig)
}



