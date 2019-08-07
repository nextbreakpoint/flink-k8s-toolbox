package com.nextbreakpoint.command

import com.nextbreakpoint.common.Command
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.ConnectionConfig
import com.nextbreakpoint.common.model.StopOptions

class ClusterStop : Command<StopOptions>(DefaultWebClientFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: StopOptions
    ) {
        Commands.putJson(super.factory, connectionConfig, "/cluster/$clusterName/stop", args)
    }
}

