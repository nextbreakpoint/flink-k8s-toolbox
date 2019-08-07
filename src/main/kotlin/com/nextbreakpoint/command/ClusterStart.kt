package com.nextbreakpoint.command

import com.nextbreakpoint.common.Command
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.ConnectionConfig
import com.nextbreakpoint.common.model.StartOptions

class ClusterStart : Command<StartOptions>(DefaultWebClientFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: StartOptions
    ) {
        Commands.putJson(super.factory, connectionConfig, "/cluster/$clusterName/start", args)
    }
}

