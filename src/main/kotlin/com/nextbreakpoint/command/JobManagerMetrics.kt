package com.nextbreakpoint.command

import com.nextbreakpoint.common.CommandNoArgs
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.Address

class JobManagerMetrics : CommandNoArgs(DefaultWebClientFactory) {
    override fun run(
        address: Address,
        clusterName: String
    ) {
        Commands.get(factory, address, "/cluster/$clusterName/jobmanager/metrics")
    }
}

