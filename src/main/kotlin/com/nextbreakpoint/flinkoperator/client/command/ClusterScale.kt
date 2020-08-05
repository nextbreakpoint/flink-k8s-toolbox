package com.nextbreakpoint.flinkoperator.client.command

import com.nextbreakpoint.flinkoperator.client.core.ClusterCommand
import com.nextbreakpoint.flinkoperator.client.factory.WebClientDefaultFactory
import com.nextbreakpoint.flinkoperator.client.core.HttpUtils
import com.nextbreakpoint.flinkoperator.common.ConnectionConfig
import com.nextbreakpoint.flinkoperator.common.ScaleOptions

class ClusterScale : ClusterCommand<ScaleOptions>(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: ScaleOptions
    ) {
        HttpUtils.putJson(factory, connectionConfig, "/cluster/$clusterName/scale", args)
    }
}

