package com.nextbreakpoint.flink.cli.core

import com.nextbreakpoint.flink.common.ConnectionConfig

interface OperatorCommand {
    fun run(connectionConfig: ConnectionConfig)
}



