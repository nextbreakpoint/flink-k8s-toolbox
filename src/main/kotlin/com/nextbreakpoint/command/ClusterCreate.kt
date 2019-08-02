package com.nextbreakpoint.command

import com.nextbreakpoint.common.Command
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.Address

class ClusterCreate : Command<String>(DefaultWebClientFactory) {
    override fun run(
        address: Address,
        clusterName: String,
        args: String
    ) {
        Commands.postText(super.factory, address, "/cluster/$clusterName", args)
    }
}

