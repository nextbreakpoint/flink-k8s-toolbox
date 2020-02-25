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

class ClusterRunning : Task {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterRunning::class.simpleName)
    }

    override fun onExecuting(context: TaskContext): TaskResult<String> {
        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Running)
        Status.setTaskAttempts(context.flinkCluster, 0)

        val taskManagers = context.flinkCluster.spec?.taskManagers ?: 0
        val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
        Status.setTaskManagers(context.flinkCluster, taskManagers)
        Status.setTaskSlots(context.flinkCluster, taskSlots)
        Status.setJobParallelism(context.flinkCluster, taskManagers * taskSlots)

        val serviceMode = context.flinkCluster.spec?.jobManager?.serviceMode
        Status.setServiceMode(context.flinkCluster, serviceMode)

        val savepointMode = context.flinkCluster.spec?.operator?.savepointMode
        Status.setSavepointMode(context.flinkCluster, savepointMode)

        val jobRestartPolicy = context.flinkCluster.spec?.operator?.jobRestartPolicy
        Status.setJobRestartPolicy(context.flinkCluster, jobRestartPolicy)

        Status.resetSavepointRequest(context.flinkCluster)

        return skip(context.flinkCluster, "Cluster running")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction != ManualAction.STOP) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
        }

        if (isStoppingCluster(context)) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            return next(context.flinkCluster, "Stopping cluster...")
        }

        if (isUpdatingCluster(context)) {
            return next(context.flinkCluster, "Resource changed. Restarting...")
        }

        if (isTerminatingJob(context)) {
            return next(context.flinkCluster, "Job finished. Stopping...")
        }

        if (isClusterNotRunning(context)) {
            return fail(context.flinkCluster, "Cluster not running...")
        }

        if (isCreatingSavepoint(context)) {
            return next(context.flinkCluster, "Creating savepoint...")
        }

        if (isRescalingCluster(context)) {
            return next(context.flinkCluster, "Rescaling cluster...")
        }

        return next(context.flinkCluster, "Cluster running")
    }

    private fun isRescalingCluster(context: TaskContext): Boolean {
        val taskmanagerStatefulset = context.resources.taskmanagerStatefulSets[context.clusterId]
        val actualTaskManagers = taskmanagerStatefulset?.status?.replicas ?: 0
        val desiredTaskManagers = context.flinkCluster.spec?.taskManagers ?: 1
        val currentTaskManagers = context.flinkCluster.status?.taskManagers ?: 1
        val currentTaskSlots = context.flinkCluster.status?.taskSlots ?: 1

        if (actualTaskManagers == desiredTaskManagers && currentTaskManagers == desiredTaskManagers) {
            // cluster doesn't need rescaling
            return false
        }

        val clusterScaling = ClusterScaling(taskManagers = desiredTaskManagers, taskSlots = currentTaskSlots)
        val result = context.scaleCluster(context.clusterId, clusterScaling)

        // cluster scale changed
        return result.isCompleted()
    }

    private fun isCreatingSavepoint(context: TaskContext): Boolean {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            // job is not defined
            return false
        }

        val nextTask = Status.getNextOperatorTask(context.flinkCluster)

        if (nextTask != null) {
            // there are still tasks in the progress
            return false
        }

        val savepointMode = Status.getSavepointMode(context.flinkCluster)

        if (savepointMode?.toUpperCase() != "AUTOMATIC") {
            // automatic savepoints not enabled
            return false
        }

        val savepointIntervalInSeconds = Configuration.getSavepointInterval(context.flinkCluster)

        if (context.timeSinceLastSavepointRequestInSeconds() < savepointIntervalInSeconds) {
            // not time yet for creating a savepoint
            return false
        }

        Status.appendTasks(
            context.flinkCluster,
            listOf(
                ClusterTask.CreatingSavepoint,
                ClusterTask.TriggerSavepoint,
                ClusterTask.ClusterRunning
            )
        )

        // create a new savepoint
        return true
    }

    private fun isClusterNotRunning(context: TaskContext): Boolean {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            // job is not defined
            return false
        }

        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Running) {
            // process only a cluster when not running
            return false
        }

        val nextTask = Status.getNextOperatorTask(context.flinkCluster)

        if (nextTask != null) {
            // there are still tasks in the progress
            return false
        }

        val attempts = Status.getTaskAttempts(context.flinkCluster)

        val clusterRunning = context.isClusterRunning(context.clusterId)

        if (clusterRunning.isCompleted()) {
            if (attempts > 0) {
                // prevent updating status when not necessary
                Status.setTaskAttempts(context.flinkCluster, 0)
            }
            // cluster not running
            return false
        }

        if (clusterRunning.output) {
            // job has finished
            return false
        }

        logger.info("[name=${context.flinkCluster.metadata.name}] Job not running...")

        Status.setTaskAttempts(context.flinkCluster, attempts + 1)

        if (attempts < 3) {
            // abort only after 3 attempts
            return false
        }

        // cluster not running
        return true
    }

    private fun isTerminatingJob(context: TaskContext): Boolean {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            // job is not defined
            return false
        }

        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Running) {
            // process only a cluster when not running
            return false
        }

        val nextTask = Status.getNextOperatorTask(context.flinkCluster)

        if (nextTask != null) {
            // there are still tasks in the progress
            return false
        }

        val clusterRunning = context.isClusterRunning(context.clusterId)

        if (!clusterRunning.isCompleted()) {
            // cluster not running
            return false
        }

        if (!clusterRunning.output) {
            // job is still running
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

        // terminate job
        return true
    }

    private fun isUpdatingCluster(context: TaskContext): Boolean {
        if (Status.getClusterStatus(context.flinkCluster) != ClusterStatus.Running) {
            // attempt updating a cluster only when not running
            return false
        }

        val changes = computeChanges(context.flinkCluster)

        if (changes.isNotEmpty()) {
            logger.info("[name=${context.flinkCluster.metadata.name}] Detected changes: ${changes.joinToString(separator = ",")}")
        }

        if (changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")) {
            if (java.lang.Boolean.getBoolean("disableReplaceStrategy")) {
                logger.info("[name=${context.flinkCluster.metadata.name}] Replace strategy not enabled")

                Status.appendTasks(context.flinkCluster,
                    listOf(
                        ClusterTask.StoppingCluster,
                        ClusterTask.CancelJob,
                        ClusterTask.TerminatePods,
                        ClusterTask.StartingCluster,
                        ClusterTask.CreateResources,
                        ClusterTask.CreateBootstrapJob,
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
                    ClusterTask.CancelJob,
                    ClusterTask.RefreshStatus,
                    ClusterTask.CreateBootstrapJob,
                    ClusterTask.ClusterRunning
                )
            )
        }

        // update cluster if something changed
        return changes.isNotEmpty()
    }
}