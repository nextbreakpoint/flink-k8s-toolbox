package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
import com.nextbreakpoint.flinkoperator.controller.core.Configuration
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import org.apache.log4j.Logger

class ClusterRunning : Task {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterRunning::class.simpleName)
    }

    override fun onExecuting(context: TaskContext): Result<String> {
        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Running)
        Status.setTaskAttempts(context.flinkCluster, 0)

        val taskManagers = context.flinkCluster.spec?.taskManagers ?: 0
        val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
        Status.setTaskManagers(context.flinkCluster, taskManagers)
        Status.setTaskSlots(context.flinkCluster, taskSlots)
        Status.setJobParallelism(context.flinkCluster, taskManagers * taskSlots)

        Status.updateSavepointTimestamp(context.flinkCluster)

        return taskCompletedWithOutput(context.flinkCluster, "Status has been updated")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        return taskCompletedWithOutput(context.flinkCluster, "Cluster is running...")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction != ManualAction.STOP) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
        }

        if (isStoppingCluster(context)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Stopping cluster...")
        }

        if (isUpdatingCluster(context)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Resource changed. Restarting...")
        }

        if (isTerminatingJob(context)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Job finished. Stopping...")
        }

        if (isClusterNotRunning(context)) {
            return taskFailedWithOutput(context.flinkCluster, "Cluster failure...")
        }

        if (isCreatingSavepoint(context)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Creating savepoint...")
        }

        if (isRescalingCluster(context)) {
            return taskAwaitingWithOutput(context.flinkCluster, "Rescaling cluster...")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Cluster running")
    }

    private fun isRescalingCluster(context: TaskContext): Boolean {
        val taskmanagerStatefulset = context.resources.taskmanagerStatefulSets[context.clusterId]
        val actualTaskManagers = taskmanagerStatefulset?.status?.replicas ?: 0
        val desiredTaskManagers = context.flinkCluster.spec?.taskManagers ?: 1
        val currentTaskManagers = context.flinkCluster.status?.taskManagers ?: 1
        val currentTaskSlots = context.flinkCluster.status?.taskSlots ?: 1

        if (actualTaskManagers == desiredTaskManagers && currentTaskManagers == desiredTaskManagers) {
            return false
        }

        val clusterScaling = ClusterScaling(taskManagers = desiredTaskManagers, taskSlots = currentTaskSlots)
        val result = context.scaleCluster(context.clusterId, clusterScaling)

        return result.isCompleted()
    }

    private fun isCreatingSavepoint(context: TaskContext): Boolean {
        val nextTask = Status.getNextOperatorTask(context.flinkCluster)

        if (nextTask != null) {
            return false
        }

        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return false
        }

        val savepointMode = Configuration.getSavepointMode(context.flinkCluster)

        if (savepointMode.toUpperCase() != "AUTOMATIC") {
            return false
        }

        val savepointIntervalInSeconds = Configuration.getSavepointInterval(context.flinkCluster)

        if (context.timeSinceLastSavepointInSeconds() > savepointIntervalInSeconds) {
            Status.appendTasks(
                context.flinkCluster,
                listOf(
                    ClusterTask.CreatingSavepoint,
                    ClusterTask.TriggerSavepoint,
                    ClusterTask.ClusterRunning
                )
            )

            return true
        }

        return false
    }

    private fun isClusterNotRunning(context: TaskContext): Boolean {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return false
        }

        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Running) {
            return false
        }

        val nextTask = Status.getNextOperatorTask(context.flinkCluster)

        if (nextTask != null) {
            return false
        }

        val attempts = Status.getTaskAttempts(context.flinkCluster)

        val clusterRunning = context.isClusterRunning(context.clusterId)

        if (clusterRunning.isCompleted()) {
            if (attempts > 0) {
                // prevent updating status when not necessary
                Status.setTaskAttempts(context.flinkCluster, 0)
            }

            return false
        }

        if (clusterRunning.output) {
            return false
        }

        logger.info("[name=${context.flinkCluster.metadata.name}] Job not running...")

        Status.setTaskAttempts(context.flinkCluster, attempts + 1)

        if (attempts < 3) {
            return false
        }

        return true
    }

    private fun isTerminatingJob(context: TaskContext): Boolean {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return false
        }

        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Running) {
            return false
        }

        val nextTask = Status.getNextOperatorTask(context.flinkCluster)

        if (nextTask != null) {
            return false
        }

        val clusterRunning = context.isClusterRunning(context.clusterId)

        if (!clusterRunning.isCompleted()) {
            return false
        }

        if (!clusterRunning.output) {
            return false
        }

        Status.appendTasks(context.flinkCluster,
            listOf(
                ClusterTask.StoppingCluster,
                ClusterTask.TerminatePods,
                ClusterTask.SuspendCluster,
                ClusterTask.ClusterHalted
            )
        )

        return true
    }

    private fun isUpdatingCluster(context: TaskContext): Boolean {
        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Running) {
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

                Status.appendTasks(context.flinkCluster,
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.CancelJob,
                        ClusterTask.TerminatePods,
//                        ClusterTask.DeleteResources,
                        ClusterTask.StartingCluster,
                        ClusterTask.CreateResources,
//                        ClusterTask.DeleteBootstrapJob,
                        ClusterTask.CreateBootstrapJob,
                        ClusterTask.StartJob,
                        ClusterTask.ClusterRunning
                    )
                )
            } else {
                logger.info("[name=${context.flinkCluster.metadata.name}] Replace strategy enabled")

                Status.appendTasks(context.flinkCluster,
                    listOf(
                        ClusterTask.UpdatingCluster,
                        ClusterTask.CancelJob,
                        ClusterTask.CreateResources,
//                        ClusterTask.DeleteBootstrapJob,
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
                    ClusterTask.CancelJob,
//                    ClusterTask.DeleteBootstrapJob,
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

    private fun isStoppingCluster(context: TaskContext): Boolean {
        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction != ManualAction.STOP) {
            return false
        }

        logger.info("[name=${context.flinkCluster.metadata.name}] User stopped the cluster...")
        val withoutSavepoint = Annotations.isWithSavepoint(context.flinkCluster)
        val deleteResources = Annotations.isDeleteResources(context.flinkCluster)
        val options = StopOptions(withoutSavepoint = withoutSavepoint, deleteResources = deleteResources)
        val result = context.stopCluster(context.clusterId, options)
        return result.isCompleted()
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}