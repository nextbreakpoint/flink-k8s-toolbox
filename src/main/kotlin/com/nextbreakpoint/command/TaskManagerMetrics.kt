package com.nextbreakpoint.command

import com.nextbreakpoint.common.Command
import com.nextbreakpoint.common.Commands
import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.model.ConnectionConfig
import com.nextbreakpoint.common.model.TaskManagerId

class TaskManagerMetrics : Command<TaskManagerId>(DefaultWebClientFactory) {
    override fun run(
        connectionConfig: ConnectionConfig,
        clusterName: String,
        args: TaskManagerId
    ) {
        Commands.get(factory, connectionConfig, "/cluster/$clusterName/taskmanager/${args.taskmanagerId}/metrics")
    }
}

