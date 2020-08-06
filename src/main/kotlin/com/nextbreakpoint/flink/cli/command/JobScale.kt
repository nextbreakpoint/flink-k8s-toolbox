package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.core.JobCommand
import com.nextbreakpoint.flink.common.ConnectionConfig
import com.nextbreakpoint.flink.common.ScaleJobOptions

class JobScale : JobCommand<ScaleJobOptions>(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        jobName: String,
        args: ScaleJobOptions
    ) {
        HttpUtils.putJson(factory, connectionConfig, "/clusters/$clusterName/jobs/$jobName/scale", args)
    }
}

