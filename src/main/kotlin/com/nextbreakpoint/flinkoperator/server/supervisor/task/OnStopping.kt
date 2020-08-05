package com.nextbreakpoint.flinkoperator.server.supervisor.task

import com.nextbreakpoint.flinkoperator.server.supervisor.core.Task
import com.nextbreakpoint.flinkoperator.server.supervisor.core.TaskContext

class OnStopping : Task() {
    override fun execute(context: TaskContext) {
        if (context.hasTaskTimedOut()) {
            context.onTaskTimeOut()
            return
        }

        if (context.mustTerminateResources()) {
            if (context.terminateCluster()) {
                context.onClusterTerminated()
                return
            }
        } else {
            if (context.suspendCluster()) {
                context.onClusterSuspended()
                return
            }
        }
    }
}