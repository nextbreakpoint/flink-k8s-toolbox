package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.core.OperatorCommand
import com.nextbreakpoint.flink.common.ConnectionConfig

class ClustersList : OperatorCommand(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig
    ) {
        HttpUtils.get(factory, connectionConfig, "/clusters")
    }
}

