package com.nextbreakpoint.command

import com.nextbreakpoint.common.CommandNoArgs
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.ConnectionConfig

class SavepointTrigger : CommandNoArgs(DefaultWebClientFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String
    ) {
        Commands.putJson(factory, connectionConfig, "/cluster/$clusterName/savepoint", null)
    }
}

