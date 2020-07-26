package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

class OnStarting : Task() {
    private val actions = setOf(
        ManualAction.STOP,
        ManualAction.FORGET_SAVEPOINT
    )

    override fun execute(context: TaskContext) {
        if (context.isResourceDeleted()) {
            context.onResourceDeleted()
            return
        }

        if (context.hasTaskTimedOut()) {
            context.onTaskTimeOut()
            return
        }

        if (context.isManualActionPresent()) {
            context.executeManualAction(actions)
            return
        }

        if (context.hasResourceChanged()) {
            context.onResourceChanged()
            return
        }

        context.ensurePodsExists()
        context.ensureServiceExist()

        if (context.startCluster()) {
            context.onClusterStarted()
            return
        }
    }
}