package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.core.JobCommand
import com.nextbreakpoint.flink.common.ConnectionConfig

class SavepointForget : JobCommand<Void?>(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        jobName: String,
        unused: Void?
    ) {
        HttpUtils.delete(factory, connectionConfig, "/clusters/$clusterName/jobs/$jobName/savepoint")
    }
}

