package com.nextbreakpoint.flinkoperator.server.supervisor.task

import com.nextbreakpoint.flinkoperator.common.ManualAction
import com.nextbreakpoint.flinkoperator.server.supervisor.core.Task
import com.nextbreakpoint.flinkoperator.server.supervisor.core.TaskContext

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

        if (context.hasScaleChanged()) {
            context.onResourceScaled()
            return
        }

        if (!context.ensurePodsExists()) {
            return
        }

        if (!context.ensureServiceExist()) {
            return
        }

        if (context.startCluster()) {
            context.onClusterStarted()
            return
        }
    }
}