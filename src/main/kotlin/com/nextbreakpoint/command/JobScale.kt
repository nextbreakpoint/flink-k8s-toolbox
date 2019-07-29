package com.nextbreakpoint.command

import com.nextbreakpoint.common.Command
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.Address
import com.nextbreakpoint.common.model.ScaleOptions

class JobScale : Command<ScaleOptions>(DefaultWebClientFactory) {
    override fun run(
        address: Address,
        clusterName: String,
        args: ScaleOptions
    ) {
        Commands.putJson(factory, address, "/cluster/$clusterName/scale", args)
    }
}

