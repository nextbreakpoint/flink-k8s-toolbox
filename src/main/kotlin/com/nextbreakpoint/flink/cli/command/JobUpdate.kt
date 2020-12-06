package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.core.JobCommand
import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.common.ConnectionConfig

class JobUpdate(private val factory: WebClientFactory = WebClientDefaultFactory) : JobCommand<String> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, jobName: String, args: String) {
        HttpUtils.putJson(factory, connectionConfig, "/clusters/$clusterName/jobs/$jobName", args)
    }
}

