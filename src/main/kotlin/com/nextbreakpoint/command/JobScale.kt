package com.nextbreakpoint.command

import com.nextbreakpoint.common.Command
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.ConnectionConfig
import com.nextbreakpoint.common.model.ScaleOptions

class JobScale : Command<ScaleOptions>(DefaultWebClientFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: ScaleOptions
    ) {
        Commands.putJson(factory, connectionConfig, "/cluster/$clusterName/scale", args)
    }
}

