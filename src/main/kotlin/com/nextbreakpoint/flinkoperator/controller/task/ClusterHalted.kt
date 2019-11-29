package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
import com.nextbreakpoint.flinkoperator.controller.core.Configuration
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import org.apache.log4j.Logger

class ClusterHalted : Task {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterHalted::class.simpleName)
    }

    override fun onExecuting(context: TaskContext): TaskResult<String> {
        Status.setTaskAttempts(context.flinkCluster, 0)

        return skip(context.flinkCluster, "Cluster halted")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction != ManualAction.START && manualAction != ManualAction.STOP) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
        }

        if (isStartingCluster(context)) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            return next(context.flinkCluster, "Starting cluster...")
        }

        if (isStoppingCluster(context)) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            return next(context.flinkCluster, "Stopping cluster...")
        }

        if (isUpdatingCluster(context)) {
            return next(context.flinkCluster, "Resource changed. Restarting...")
        }

        if (isClusterRunning(context)) {
            return next(context.flinkCluster, "Cluster is running...")
        }

        if (isRestartingJob(context)) {
            return next(context.flinkCluster, "Restarting job...")
        }

        if (Status.getClusterStatus(context.flinkCluster) == ClusterStatus.Failed) {
            logger.warn("[name=${context.flinkCluster.metadata.name}] Not healthy")
        }

        return next(context.flinkCluster, "Cluster halted")
    }

    private fun isRestartingJob(context: TaskContext): Boolean {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return false
        }

        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Failed) {
            return false
        }

        val nextTask = Status.getNextOperatorTask(context.flinkCluster)

        if (nextTask != null) {
            return false
        }

        val restartPolicy = Status.getJobRestartPolicy(context.flinkCluster)

        if (restartPolicy?.toUpperCase() != "ALWAYS") {
            return false
        }

        val attempts = Status.getTaskAttempts(context.flinkCluster)

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val clusterReady = context.isClusterReady(context.clusterId, clusterScaling)

        if (!clusterReady.isCompleted()) {
            if (attempts > 0) {
                // prevent updating status when not necessary
                Status.setTaskAttempts(context.flinkCluster, 0)
            }

            return false
        }

        logger.info("[name=${context.flinkCluster.metadata.name}] Cluster is ready...")

        Status.setTaskAttempts(context.flinkCluster, attempts + 1)

        if (attempts < 3) {
            return false
        }

        Status.appendTasks(
            context.flinkCluster, listOf(
                ClusterTask.CreateBootstrapJob,
                ClusterTask.ClusterRunning
            )
        )

        return true
    }

    private fun isClusterRunning(context: TaskContext): Boolean {
        val clusterRunning = context.isClusterRunning(context.clusterId)

        if (clusterRunning.isCompleted()) {
            Status.appendTasks(
                context.flinkCluster, listOf(
                    ClusterTask.ClusterRunning
                )
            )

            return true
        }

        return false
    }

    private fun isUpdatingCluster(context: TaskContext): Boolean {
        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Failed) {
            return false
        }

        val changes = computeChanges(context.flinkCluster)

        if (changes.isNotEmpty()) {
            logger.info("[name=${context.flinkCluster.metadata.name}] Detected changes: ${changes.joinToString(separator = ",")}")
        }

        if (changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")) {
            if (java.lang.Boolean.getBoolean("disableReplaceStrategy")) {
                logger.info("[name=${context.flinkCluster.metadata.name}] Replace strategy not enabled")

                Status.appendTasks(
                    context.flinkCluster,
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.TerminatePods,
                        ClusterTask.StartingCluster,
                        ClusterTask.CreateResources,
                        ClusterTask.CreateBootstrapJob,
                        ClusterTask.ClusterRunning
                    )
                )
            } else {
                logger.info("[name=${context.flinkCluster.metadata.name}] Replace strategy enabled")

                Status.appendTasks(
                    context.flinkCluster,
                    listOf(
                        ClusterTask.UpdatingCluster,
                        ClusterTask.CreateResources,
                        ClusterTask.CreateBootstrapJob,
                        ClusterTask.ClusterRunning
                    )
                )
            }
        } else if (changes.contains("BOOTSTRAP")) {
            Status.appendTasks(
                context.flinkCluster,
                listOf(
                    ClusterTask.UpdatingCluster,
                    ClusterTask.CreateResources,
                    ClusterTask.CreateBootstrapJob,
                    ClusterTask.ClusterRunning
                )
            )
        }

        return changes.isNotEmpty()
    }
}