package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import org.apache.log4j.Logger

class OnUpdating(logger: Logger) : Task(logger) {
    override fun execute(context: TaskContext) {
        if (context.hasBeenDeleted()) {
            context.setDeleteResources(true)
            context.resetManualAction()
            context.setClusterStatus(ClusterStatus.Stopping)

            return
        }

        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TASK_TIMEOUT) {
            logger.error("Cluster not updated after $seconds seconds")

            context.resetSavepointRequest()
            context.setClusterStatus(ClusterStatus.Failed)

            return
        }

        val changes = context.computeChanges()

        if (changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")) {
            if (terminate(context)) {
                context.updateStatus()
                context.updateDigests()
                context.setClusterStatus(ClusterStatus.Starting)

                return
            }
        } else if (changes.contains("BOOTSTRAP")) {
            if (cancel(context)) {
                context.updateStatus()
                context.updateDigests()
                context.setClusterStatus(ClusterStatus.Starting)

                return
            }
        } else {
            context.setClusterStatus(ClusterStatus.Starting)

            return
        }
    }
}