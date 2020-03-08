package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import org.apache.log4j.Logger

class OnScaling(logger: Logger) : Task(logger) {
    override fun execute(context: TaskContext) {
        if (context.hasBeenDeleted()) {
            context.setDeleteResources(true)
            context.resetManualAction()
            context.setClusterStatus(ClusterStatus.Stopping)

            return
        }

        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TASK_TIMEOUT) {
            logger.error("Cluster not scaled after $seconds seconds")

            context.resetSavepointRequest()
            context.setClusterStatus(ClusterStatus.Failed)

            return
        }

        if (cancel(context)) {
            context.rescaleCluster()

            if (context.getTaskManagers() == 0) {
                context.setClusterStatus(ClusterStatus.Stopping)
            } else {
                context.setClusterStatus(ClusterStatus.Starting)
            }
        }
    }
}