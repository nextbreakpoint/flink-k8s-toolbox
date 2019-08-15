package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.RemoteCommand
import com.nextbreakpoint.flinkoperator.cli.HttpUtils
import com.nextbreakpoint.flinkoperator.cli.DefaultWebClientFactory
import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig
import com.nextbreakpoint.flinkoperator.common.model.StartOptions

class ClusterStart : RemoteCommand<StartOptions>(DefaultWebClientFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: StartOptions
    ) {
        HttpUtils.putJson(super.factory, connectionConfig, "/cluster/$clusterName/start", args)
    }
}

