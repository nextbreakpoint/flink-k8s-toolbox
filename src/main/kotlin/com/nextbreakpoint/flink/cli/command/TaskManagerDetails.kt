package com.nextbreakpoint.flink.cli.command

import com.nextbreakpoint.flink.cli.core.ClusterCommand
import com.nextbreakpoint.flink.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.cli.core.HttpUtils
import com.nextbreakpoint.flink.common.ConnectionConfig
import com.nextbreakpoint.flink.common.TaskManagerId

class TaskManagerDetails : ClusterCommand<TaskManagerId>(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: TaskManagerId
    ) {
        HttpUtils.get(factory, connectionConfig, "/clusters/$clusterName/taskmanagers/${args.taskmanagerId}/details")
    }
}

