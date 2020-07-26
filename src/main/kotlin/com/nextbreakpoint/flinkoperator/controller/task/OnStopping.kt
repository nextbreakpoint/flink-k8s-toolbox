package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

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