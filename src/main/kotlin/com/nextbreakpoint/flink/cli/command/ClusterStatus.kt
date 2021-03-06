package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.ClusterCommand
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.common.ConnectionConfig

class ClusterStatus(private val factory: WebClientFactory = WebClientDefaultFactory) : ClusterCommand<Void?> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, unused: Void?) {
        HttpUtils.get(factory, connectionConfig, "/api/v1/clusters/$clusterName/status")
    }
}

