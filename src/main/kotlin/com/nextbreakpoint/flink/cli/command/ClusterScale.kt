package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.ClusterCommand
import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.common.ConnectionConfig
import com.nextbreakpoint.flink.common.ScaleClusterOptions

class ClusterScale : ClusterCommand<ScaleClusterOptions>(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: ScaleClusterOptions
    ) {
        HttpUtils.putJson(factory, connectionConfig, "/clusters/$clusterName/scale", args)
    }
}

