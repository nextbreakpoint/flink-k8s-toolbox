package com.nextbreakpoint.flinkoperator.server.supervisor.task

import com.nextbreakpoint.flinkoperator.common.ManualAction
import com.nextbreakpoint.flinkoperator.server.supervisor.core.Task
import com.nextbreakpoint.flinkoperator.server.supervisor.core.TaskContext

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

        if (context.hasJobStopped()) {
            context.onJobStopped()
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