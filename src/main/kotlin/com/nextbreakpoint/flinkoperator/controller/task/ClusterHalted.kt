package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
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

    override fun onExecuting(context: TaskContext): Result<String> {
        Status.setTaskAttempts(context.flinkCluster, 0)

        return taskCompletedWithOutput(context.flinkCluster, "Nothing to do")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        return taskCompletedWithOutput(context.flinkCluster, "Cluster is idle...")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction != ManualAction.START) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
        }

        if (manualAction == ManualAction.START) {
            logger.info("[name=${context.flinkCluster.metadata.name}] User started the cluster...")

            if (isClusterStarting(context)) {
                Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
                return taskAwaitingWithOutput(context.flinkCluster, "Starting cluster...")
            }
        }

        if (isResourceChanged(context)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Resource changed. Restarting...")
        }

        if (isClusterRunning(context)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Cluster is running...")
        }

        if (isJobRestarting(context)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Restarting job...")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Cluster halted")
    }

    private fun isJobRestarting(context: TaskContext): Boolean {
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

        val restartPolicy = Configuration.getJobRestartPolicy(context.flinkCluster)

        if (restartPolicy.toUpperCase() != "ALWAYS") {
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
                ClusterTask.DeleteBootstrapJob,
                ClusterTask.CreateBootstrapJob,
                ClusterTask.StartJob,
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

    private fun isResourceChanged(context: TaskContext): Boolean {
        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Failed && Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Suspended) {
            return false
        }

        val changes = computeChanges(context)

        if (changes.isNotEmpty()) {
            logger.info("[name=${context.flinkCluster.metadata.name}] Detected changes: ${changes.joinToString(separator = ",")}")

            updateDigests(context)
        }

        if (changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")) {
            if (java.lang.Boolean.getBoolean("disableReplaceStrategy")) {
                logger.info("[name=${context.flinkCluster.metadata.name}] Replace strategy not enabled")

                Status.appendTasks(
                    context.flinkCluster,
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.TerminatePods,
                        ClusterTask.DeleteResources,
                        ClusterTask.StartingCluster,
                        ClusterTask.CreateResources,
                        ClusterTask.DeleteBootstrapJob,
                        ClusterTask.CreateBootstrapJob,
                        ClusterTask.StartJob,
                        ClusterTask.ClusterRunning
                    )
                )
            } else {
                logger.info("[name=${context.flinkCluster.metadata.name}] Replace strategy enabled")

                Status.appendTasks(
                    context.flinkCluster,
                    listOf(
                        ClusterTask.UpdatingCluster,
                        ClusterTask.ReplaceResources,
                        ClusterTask.DeleteBootstrapJob,
                        ClusterTask.CreateBootstrapJob,
                        ClusterTask.StartJob,
                        ClusterTask.ClusterRunning
                    )
                )
            }
        } else if (changes.contains("BOOTSTRAP")) {
            Status.appendTasks(
                context.flinkCluster,
                listOf(
                    ClusterTask.UpdatingCluster,
                    ClusterTask.DeleteBootstrapJob,
                    ClusterTask.CreateBootstrapJob,
                    ClusterTask.StartJob,
                    ClusterTask.ClusterRunning
                )
            )
        }

        return changes.isNotEmpty()
    }

    private fun computeChanges(context: TaskContext): MutableList<String> {
        val jobManagerDigest = Status.getJobManagerDigest(context.flinkCluster)
        val taskManagerDigest = Status.getTaskManagerDigest(context.flinkCluster)
        val flinkImageDigest = Status.getRuntimeDigest(context.flinkCluster)
        val flinkJobDigest = Status.getBootstrapDigest(context.flinkCluster)

        val actualJobManagerDigest = ClusterResource.computeDigest(context.flinkCluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(context.flinkCluster.spec?.taskManager)
        val actualRuntimeDigest = ClusterResource.computeDigest(context.flinkCluster.spec?.runtime)
        val actualBootstrapDigest = ClusterResource.computeDigest(context.flinkCluster.spec?.bootstrap)

        val changes = mutableListOf<String>()

        if (jobManagerDigest != actualJobManagerDigest) {
            changes.add("JOB_MANAGER")
        }

        if (taskManagerDigest != actualTaskManagerDigest) {
            changes.add("TASK_MANAGER")
        }

        if (flinkImageDigest != actualRuntimeDigest) {
            changes.add("RUNTIME")
        }

        if (flinkJobDigest != actualBootstrapDigest) {
            changes.add("BOOTSTRAP")
        }

        return changes
    }

    private fun updateDigests(context: TaskContext) {
        val actualJobManagerDigest = ClusterResource.computeDigest(context.flinkCluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(context.flinkCluster.spec?.taskManager)
        val actualRuntimeDigest = ClusterResource.computeDigest(context.flinkCluster.spec?.runtime)
        val actualBootstrapDigest = ClusterResource.computeDigest(context.flinkCluster.spec?.bootstrap)

        Status.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
        Status.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
        Status.setRuntimeDigest(context.flinkCluster, actualRuntimeDigest)
        Status.setBootstrapDigest(context.flinkCluster, actualBootstrapDigest)
    }

    private fun isClusterStarting(context: TaskContext): Boolean {
        val withoutSavepoint = Annotations.isWithSavepoint(context.flinkCluster)
        val options = StartOptions(withoutSavepoint = withoutSavepoint)
        val result = context.startCluster(context.clusterId, options)
        return result.isCompleted()
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}