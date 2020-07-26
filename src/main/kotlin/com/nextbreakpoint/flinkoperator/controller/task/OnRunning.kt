package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

class OnRunning : Task() {
    private val actions = setOf(
        ManualAction.START,
        ManualAction.STOP,
        ManualAction.FORGET_SAVEPOINT,
        ManualAction.TRIGGER_SAVEPOINT
    )

    override fun execute(context: TaskContext) {
        if (context.isResourceDeleted()) {
            context.onResourceDeleted()
            return
        }

        if (!context.resetCluster()) {
            return
        }

        if (context.hasResourceDiverged()) {
            context.onResourceDiverged()
            return
        }

        if (context.hasJobFinished()) {
            context.onJobFinished()
            return
        }

        if (context.hasJobFailed()) {
            context.onJobFailed()
            return
        }

        if (context.isManualActionPresent()) {
            context.executeManualAction(actions, true)
            return
        }

        if (context.hasResourceChanged()) {
            context.onResourceChanged()
            return
        }

        if (context.hasScaleChanged()) {
            context.onResourceScaled()
            return
        }

        context.updateSavepoint()
    }
}