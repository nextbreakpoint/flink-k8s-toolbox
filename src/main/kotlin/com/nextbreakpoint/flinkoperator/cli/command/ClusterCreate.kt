package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.RemoteCommand
import com.nextbreakpoint.flinkoperator.cli.HttpUtils
import com.nextbreakpoint.flinkoperator.cli.DefaultWebClientFactory
import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig

class ClusterCreate : RemoteCommand<String>(DefaultWebClientFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: String
    ) {
        HttpUtils.postText(super.factory, connectionConfig, "/cluster/$clusterName", args)
    }
}

