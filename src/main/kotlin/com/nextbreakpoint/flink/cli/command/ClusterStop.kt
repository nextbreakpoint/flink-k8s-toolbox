package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.ClusterCommand
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.common.ConnectionConfig
import com.nextbreakpoint.flink.common.StopOptions

class ClusterStop(private val factory: WebClientFactory = WebClientDefaultFactory) : ClusterCommand<StopOptions> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, args: StopOptions) {
        HttpUtils.putJson(factory, connectionConfig, "/api/v1/clusters/$clusterName/stop", args)
    }
}

