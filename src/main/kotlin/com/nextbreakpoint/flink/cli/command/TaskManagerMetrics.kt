package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.ClusterCommand
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.common.ConnectionConfig
import com.nextbreakpoint.flink.common.TaskManagerId

class TaskManagerMetrics(private val factory: WebClientFactory = WebClientDefaultFactory) : ClusterCommand<TaskManagerId> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, args: TaskManagerId) {
        HttpUtils.get(factory, connectionConfig, "/clusters/$clusterName/taskmanagers/${args.taskmanagerId}/metrics")
    }
}

