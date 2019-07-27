package com.nextbreakpoint.command

import com.nextbreakpoint.common.Command
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.Address
import com.nextbreakpoint.model.V1FlinkClusterSpec

class ClusterCreate : Command<V1FlinkClusterSpec>(DefaultWebClientFactory) {
    override fun run(
        address: Address,
        clusterName: String,
        args: V1FlinkClusterSpec
    ) {
        Commands.post(super.factory, address, "/cluster/$clusterName", args)
    }
}

