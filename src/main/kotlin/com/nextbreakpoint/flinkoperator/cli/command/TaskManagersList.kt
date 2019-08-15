package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.RemoteCommandNoArgs
import com.nextbreakpoint.flinkoperator.cli.HttpUtils
import com.nextbreakpoint.flinkoperator.cli.DefaultWebClientFactory
import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig

class TaskManagersList : RemoteCommandNoArgs(DefaultWebClientFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String
    ) {
        HttpUtils.get(factory, connectionConfig, "/cluster/$clusterName/taskmanagers")
    }
}

