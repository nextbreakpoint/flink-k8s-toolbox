package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.core.OperatorCommand
import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.common.ConnectionConfig

class DeploymentsList(private val factory: WebClientFactory = WebClientDefaultFactory) : OperatorCommand {
    override fun run(connectionConfig: ConnectionConfig) {
        HttpUtils.get(factory, connectionConfig, "/deployments")
    }
}

