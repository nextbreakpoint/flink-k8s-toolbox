package com.nextbreakpoint.flinkoperator.client.command

import com.nextbreakpoint.flinkoperator.client.factory.WebClientDefaultFactory
import com.nextbreakpoint.flinkoperator.client.core.HttpUtils
import com.nextbreakpoint.flinkoperator.client.core.OperatorCommand
import com.nextbreakpoint.flinkoperator.common.ConnectionConfig

class ClustersList : OperatorCommand(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig
    ) {
        HttpUtils.get(factory, connectionConfig, "/clusters")
    }
}

