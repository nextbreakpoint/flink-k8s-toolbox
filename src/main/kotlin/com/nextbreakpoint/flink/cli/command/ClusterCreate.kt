package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.ClusterCommand
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.common.ConnectionConfig

class ClusterCreate(private val factory: WebClientFactory = WebClientDefaultFactory) : ClusterCommand<String> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, args: String) {
        HttpUtils.postJson(factory, connectionConfig, "/clusters/$clusterName", args)
    }
}

