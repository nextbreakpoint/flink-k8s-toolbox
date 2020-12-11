package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.DeploymentCommand
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.common.ConnectionConfig

class DeploymentStatus(private val factory: WebClientFactory = WebClientDefaultFactory) : DeploymentCommand<Void?> {
    override fun run(connectionConfig: ConnectionConfig, deploymentName: String, unused: Void?) {
        HttpUtils.get(factory, connectionConfig, "/deployments/$deploymentName/status")
    }
}

