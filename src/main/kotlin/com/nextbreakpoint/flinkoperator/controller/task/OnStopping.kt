package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import org.apache.log4j.Logger

class OnStopping(logger: Logger) : Task(logger) {
    override fun execute(context: TaskContext) {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TASK_TIMEOUT) {
            logger.error("Cluster not stopped after $seconds seconds")

            context.resetSavepointRequest()
            context.setClusterStatus(ClusterStatus.Failed)

            return
        }

        if (!context.isBootstrapPresent()) {
            context.setTaskStatus(TaskStatus.Executing)

            return
        }

        val taskStatus = context.getTaskStatus()

        when (taskStatus) {
            TaskStatus.Idle -> {
                val runningResult = context.isJobRunning(context.clusterId)

                if (runningResult.isCompleted()) {
                    context.setTaskStatus(TaskStatus.Awaiting)
                } else {
                    context.setTaskStatus(TaskStatus.Executing)
                }
            }
            TaskStatus.Awaiting -> {
                if (cancel(context)) {
                    context.setTaskStatus(TaskStatus.Executing)
                }
            }
            TaskStatus.Executing -> {
                if (context.isDeleteResources()) {
                    if (terminate(context)) {
                        context.resetSavepointRequest()
                        context.setTaskStatus(TaskStatus.Idle)
                        context.setClusterStatus(ClusterStatus.Terminated)
                    }
                } else {
                    if (suspend(context)) {
                        context.resetSavepointRequest()
                        context.setTaskStatus(TaskStatus.Idle)
                        context.setClusterStatus(ClusterStatus.Suspended)
                    }
                }
            }
            else -> {}
        }
    }
}