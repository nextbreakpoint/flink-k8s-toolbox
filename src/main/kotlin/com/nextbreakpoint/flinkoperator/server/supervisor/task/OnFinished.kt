package com.nextbreakpoint.flinkoperator.server.supervisor.task

import com.nextbreakpoint.flinkoperator.common.ManualAction
import com.nextbreakpoint.flinkoperator.server.supervisor.core.Task
import com.nextbreakpoint.flinkoperator.server.supervisor.core.TaskContext

class OnFinished : Task() {
    private val actions = setOf(
        ManualAction.START,
        ManualAction.STOP,
        ManualAction.FORGET_SAVEPOINT
    )

    override fun execute(context: TaskContext) {
        if (context.isResourceDeleted()) {
            context.onResourceDeleted()
            return
        }

        if (!context.suspendCluster()) {
            return
        }

        if (context.isManualActionPresent()) {
            context.executeManualAction(actions)
            return
        }

        if (context.shouldRestart()) {
            if (context.hasResourceChanged()) {
                context.onResourceChanged()
                return
            }
        }
    }
}