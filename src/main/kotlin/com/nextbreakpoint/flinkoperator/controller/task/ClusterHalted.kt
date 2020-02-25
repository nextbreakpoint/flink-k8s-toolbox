package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
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
            return next(context.flinkCluster, "Cluster should be running...")
        }

        if (isClusterSuspended(context)) {
            return next(context.flinkCluster, "Cluster should be suspended...")
        }

        if (isClusterTerminated(context)) {
            return next(context.flinkCluster, "Cluster should be terminated...")
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
            // job is not defined
            return false
        }

        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Failed) {
            // attempt restarting a cluster only when failed
            return false
        }

        val nextTask = Status.getNextOperatorTask(context.flinkCluster)

        if (nextTask != null) {
            // there are still tasks in the progress
            return false
        }

        val restartPolicy = Status.getJobRestartPolicy(context.flinkCluster)

        if (restartPolicy?.toUpperCase() != "ALWAYS") {
            // policy doesn't allow restart
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
            // cluster not ready
            return false
        }

        logger.info("[name=${context.flinkCluster.metadata.name}] Cluster is ready...")

        Status.setTaskAttempts(context.flinkCluster, attempts + 1)

        if (attempts < 3) {
            // restart only after 3 attempts
            return false
        }

        Status.appendTasks(
            context.flinkCluster, listOf(
                ClusterTask.CreateBootstrapJob,
                ClusterTask.ClusterRunning
            )
        )

        // cluster should be restarted
        return true
    }

    private fun isClusterRunning(context: TaskContext): Boolean {
        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Failed) {
            // process cluster only when failed
            return false
        }

        val clusterRunning = context.isClusterRunning(context.clusterId)

        if (!clusterRunning.isCompleted()) {
            // cluster is not running
            return false
        }

        if (clusterRunning.output) {
            // job has finished
            return false
        }

        Status.appendTasks(
            context.flinkCluster, listOf(
                ClusterTask.ClusterRunning
            )
        )

        // cluster is running
        return true
    }

    private fun isClusterSuspended(context: TaskContext): Boolean {
        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Suspended) {
            // process cluster only when suspended
            return false
        }

        if (context.timeSinceLastUpdateInSeconds() < 300) {
            // ensure enough time passed
            return false
        }

        if (context.flinkCluster.status.activeTaskManagers == 0) {
            // resources are terminated
            return false
        }

        val clusterTerminated = context.isClusterTerminated(context.clusterId)

        if (clusterTerminated.isCompleted()) {
            // cluster is terminated
            return false
        }

        Status.appendTasks(
            context.flinkCluster, listOf(
                ClusterTask.StoppingCluster,
                ClusterTask.TerminatePods,
                ClusterTask.DeleteBootstrapJob,
                ClusterTask.SuspendCluster,
                ClusterTask.ClusterHalted
            )
        )

        // cluster should be suspended
        return true
    }

    private fun isClusterTerminated(context: TaskContext): Boolean {
        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Terminated) {
            // process cluster only when terminated
            return false
        }

        if (context.timeSinceLastUpdateInSeconds() < 300) {
            // ensure enough time passed
            return false
        }

        if (context.flinkCluster.status.activeTaskManagers == 0) {
            // resources are terminated
            return false
        }

        val clusterTerminated = context.isClusterTerminated(context.clusterId)

        if (clusterTerminated.isCompleted()) {
            // cluster is terminated
            return false
        }

        Status.appendTasks(
            context.flinkCluster, listOf(
                ClusterTask.StoppingCluster,
                ClusterTask.TerminatePods,
                ClusterTask.DeleteResources,
                ClusterTask.TerminatedCluster,
                ClusterTask.ClusterHalted
            )
        )

        // cluster should be terminated
        return true
    }

    private fun isUpdatingCluster(context: TaskContext): Boolean {
        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Failed) {
            // attempt updating a cluster only when failed
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

        // update cluster if something changed
        return changes.isNotEmpty()
    }
}