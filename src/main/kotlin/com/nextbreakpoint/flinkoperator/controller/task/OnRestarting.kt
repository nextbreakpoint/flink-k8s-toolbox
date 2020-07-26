package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

class OnRestarting : Task() {
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
            context.onClusterReadyToRestart()
            return
        }
    }
}