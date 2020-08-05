package com.nextbreakpoint.flinkoperator.client.command

import com.nextbreakpoint.flinkoperator.client.core.ClusterCommand
import com.nextbreakpoint.flinkoperator.client.factory.WebClientDefaultFactory
import com.nextbreakpoint.flinkoperator.client.core.HttpUtils
import com.nextbreakpoint.flinkoperator.common.ConnectionConfig
import com.nextbreakpoint.flinkoperator.common.TaskManagerId

class TaskManagerDetails : ClusterCommand<TaskManagerId>(WebClientDefaultFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: TaskManagerId
    ) {
        HttpUtils.get(factory, connectionConfig, "/cluster/$clusterName/taskmanager/${args.taskmanagerId}/details")
    }
}

