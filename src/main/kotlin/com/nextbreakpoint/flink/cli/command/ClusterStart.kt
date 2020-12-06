package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.ClusterCommand
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.common.ConnectionConfig
import com.nextbreakpoint.flink.common.StartOptions

class ClusterStart(private val factory: WebClientFactory = WebClientDefaultFactory) : ClusterCommand<StartOptions> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, args: StartOptions) {
        HttpUtils.putJson(factory, connectionConfig, "/clusters/$clusterName/start", args)
    }
}

