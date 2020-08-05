package com.nextbreakpoint.flinkoperator.client.command

import com.nextbreakpoint.flinkoperator.client.core.ClusterCommand
import com.nextbreakpoint.flinkoperator.client.factory.WebClientDefaultFactory
import com.nextbreakpoint.flinkoperator.client.core.HttpUtils
import com.nextbreakpoint.flinkoperator.common.ConnectionConfig
import com.nextbreakpoint.flinkoperator.common.StartOptions

class ClusterStart : ClusterCommand<StartOptions>(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: StartOptions
    ) {
        HttpUtils.putJson(factory, connectionConfig, "/cluster/$clusterName/start", args)
    }
}

