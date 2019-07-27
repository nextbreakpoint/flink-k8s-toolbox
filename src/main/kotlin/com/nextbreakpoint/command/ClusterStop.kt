package com.nextbreakpoint.command

import com.nextbreakpoint.common.Command
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.Address
import com.nextbreakpoint.common.model.StopOptions

class ClusterStop : Command<StopOptions>(DefaultWebClientFactory) {
    override fun run(
        address: Address,
        clusterName: String,
        args: StopOptions
    ) {
        Commands.put(super.factory, address, "/cluster/$clusterName/stop", args)
    }
}

