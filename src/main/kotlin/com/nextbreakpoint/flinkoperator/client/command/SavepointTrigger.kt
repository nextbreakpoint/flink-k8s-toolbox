package com.nextbreakpoint.flinkoperator.client.command

import com.nextbreakpoint.flinkoperator.client.core.ClusterCommandNoArgs
import com.nextbreakpoint.flinkoperator.client.factory.WebClientDefaultFactory
import com.nextbreakpoint.flinkoperator.client.core.HttpUtils
import com.nextbreakpoint.flinkoperator.common.ConnectionConfig

class SavepointTrigger : ClusterCommandNoArgs(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String
    ) {
        HttpUtils.putJson(factory, connectionConfig, "/cluster/$clusterName/savepoint", null)
    }
}

