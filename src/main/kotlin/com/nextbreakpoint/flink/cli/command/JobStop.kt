package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.core.JobCommand
import com.nextbreakpoint.flink.common.ConnectionConfig
import com.nextbreakpoint.flink.common.StopOptions

class JobStop : JobCommand<StopOptions>(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        jobName: String,
        args: StopOptions
    ) {
        HttpUtils.putJson(factory, connectionConfig, "/clusters/$clusterName/jobs/$jobName/stop", args)
    }
}

