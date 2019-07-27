package com.nextbreakpoint.command

import com.nextbreakpoint.common.Command
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.Address
import com.nextbreakpoint.common.model.StartOptions

class ClusterStart : Command<StartOptions>(DefaultWebClientFactory) {
    override fun run(
        address: Address,
        clusterName: String,
        args: StartOptions
    ) {
        Commands.put(super.factory, address, "/cluster/$clusterName/start", args)
    }
}

