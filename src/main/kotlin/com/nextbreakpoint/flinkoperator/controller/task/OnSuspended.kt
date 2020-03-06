package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import org.apache.log4j.Logger

class OnSuspended(logger: Logger) : Task(logger) {
    override fun execute(context: TaskContext) {
        if (context.hasBeenDeleted()) {
            context.setDeleteResources(true)
            context.resetManualAction()
            context.setClusterStatus(ClusterStatus.Stopping)

            return
        }

        val bootstrapExists = context.doesBootstrapExists()

        if (bootstrapExists) {
            val bootstrapResult = context.deleteBootstrapJob(context.clusterId)

            if (bootstrapResult.isCompleted()) {
                logger.info("Bootstrap job deleted")
            }

            return
        }

        val changes = context.computeChanges()

        if (changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")) {
            logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

            if (update(context)) {
                context.updateStatus()
                context.updateDigests()
                context.setClusterStatus(ClusterStatus.Suspended)
            }

            return
        }

        if (!suspend(context)) {
            logger.info("Suspending cluster...")
        }

        val jobmanagerStatefuleSetExists = context.doesJobManagerStatefulSetExists()
        val taskmanagerStatefulSetExists = context.doesTaskManagerStatefulSetExists()

        if (!jobmanagerStatefuleSetExists || !taskmanagerStatefulSetExists) {
            context.resetManualAction()
            context.setClusterStatus(ClusterStatus.Terminated)

            return
        }

        val manualAction = context.getManualAction()

        if (manualAction == ManualAction.START) {
            context.resetManualAction()
            context.setClusterStatus(ClusterStatus.Starting)

            return
        }

        if (manualAction == ManualAction.STOP) {
            context.resetManualAction()
            context.setClusterStatus(ClusterStatus.Stopping)

            return
        }

        if (manualAction == ManualAction.FORGET_SAVEPOINT) {
            context.resetManualAction()
            context.setSavepointPath("")

            logger.info("Savepoint forgotten")

            return
        }

        if (manualAction != ManualAction.NONE) {
            context.resetManualAction()
        }
    }
}