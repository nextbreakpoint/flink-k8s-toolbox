package com.nextbreakpoint.flinkoperator.cli.command

import com.nextbreakpoint.flinkoperator.cli.ClusterCommand
import com.nextbreakpoint.flinkoperator.cli.DefaultWebClientFactory
import com.nextbreakpoint.flinkoperator.cli.HttpUtils
import com.nextbreakpoint.flinkoperator.common.model.ConnectionConfig
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId

class TaskManagerDetails : ClusterCommand<TaskManagerId>(DefaultWebClientFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: TaskManagerId
    ) {
        HttpUtils.get(factory, connectionConfig, "/cluster/$clusterName/taskmanager/${args.taskmanagerId}/details")
    }
}

