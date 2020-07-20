package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

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

        if (context.shouldRestartJob()) {
            if (context.hasResourceChanged()) {
                context.onResourceChanged()
                return
            }
        }
    }
}