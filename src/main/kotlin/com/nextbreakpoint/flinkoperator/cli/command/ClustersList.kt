package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.DefaultWebClientFactory
import com.nextbreakpoint.flinkoperator.cli.HttpUtils
import com.nextbreakpoint.flinkoperator.cli.OperatorCommand
import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig

class ClustersList : OperatorCommand(DefaultWebClientFactory) {
    override fun run(
        connectionConfig: ConnectionConfig
    ) {
        HttpUtils.get(factory, connectionConfig, "/clusters")
    }
}

