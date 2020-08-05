package com.nextbreakpoint.flinkoperator.server.supervisor.task

import com.nextbreakpoint.flinkoperator.server.supervisor.core.Task
import com.nextbreakpoint.flinkoperator.server.supervisor.core.TaskContext

class OnCancelling : Task() {
    override fun execute(context: TaskContext) {
        if (context.isResourceDeleted()) {
            context.onResourceDeleted()
            return
        }

        if (context.hasTaskTimedOut()) {
            context.onTaskTimeOut()
            return
        }

        if (!context.resetCluster()) {
            return
        }

        if (context.cancelJob()) {
            context.onClusterReadyToStop()
            return
        }
    }
}