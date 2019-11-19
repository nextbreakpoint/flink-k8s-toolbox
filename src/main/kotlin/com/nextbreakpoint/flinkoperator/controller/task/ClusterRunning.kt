package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
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
        Status.appendTasks(context.flinkCluster, listOf())

        val taskManagers = context.flinkCluster.spec?.taskManagers ?: 0
        val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
        Status.setTaskManagers(context.flinkCluster, taskManagers)
        Status.setTaskSlots(context.flinkCluster, taskSlots)
        Status.setJobParallelism(context.flinkCluster, taskManagers * taskSlots)

        val savepointPath = context.flinkCluster.spec?.operator?.savepointPath
        Status.setSavepointPath(context.flinkCluster, savepointPath)

        Status.updateSavepointTimestamp(context.flinkCluster)

        return taskCompletedWithOutput(context.flinkCluster, "Status of cluster ${context.clusterId.name} has been updated")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        return taskCompletedWithOutput(context.flinkCluster, "Cluster ${context.clusterId.name} is running...")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        val jobManagerDigest = Status.getJobManagerDigest(context.flinkCluster)
        val taskManagerDigest = Status.getTaskManagerDigest(context.flinkCluster)
        val flinkImageDigest = Status.getRuntimeDigest(context.flinkCluster)
        val flinkJobDigest = Status.getBootstrapDigest(context.flinkCluster)

        val manualAction = Annotations.getManualAction(context.flinkCluster)
        if (manualAction == ManualAction.STOP) {
            val withoutSavepoint = Annotations.isWithSavepoint(context.flinkCluster)
            val deleteResources = Annotations.isDeleteResources(context.flinkCluster)
            val options = StopOptions(withoutSavepoint = withoutSavepoint, deleteResources = deleteResources)
            val result = context.controller.stopCluster(context.clusterId, options)
            if (result.isCompleted()) {
                Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
                return taskAwaitingWithOutput(context.flinkCluster, "")
            }
        } else {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
        }

        if (jobManagerDigest == null || taskManagerDigest == null || flinkImageDigest == null || flinkJobDigest == null) {
            return taskFailedWithOutput(context.flinkCluster, "Missing required annotations in cluster ${context.clusterId.name}")
        } else {
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

            if (changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")) {
                logger.info("Detected changes in: ${changes.joinToString(separator = ",")}")

                val clusterStatus = Status.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Running -> {
                        logger.info("Cluster ${context.clusterId.name} requires a restart")

                        Status.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        Status.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        Status.setRuntimeDigest(context.flinkCluster, actualRuntimeDigest)
                        Status.setBootstrapDigest(context.flinkCluster, actualBootstrapDigest)

                        if (java.lang.Boolean.getBoolean("disableReplaceStrategy")) {
                            Status.appendTasks(context.flinkCluster,
                                listOf(
                                    ClusterTask.StoppingCluster,
                                    ClusterTask.CancelJob,
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
                            Status.appendTasks(context.flinkCluster,
                                listOf(
                                    ClusterTask.UpdatingCluster,
                                    ClusterTask.CancelJob,
                                    ClusterTask.ReplaceResources,
                                    ClusterTask.DeleteBootstrapJob,
                                    ClusterTask.CreateBootstrapJob,
                                    ClusterTask.StartJob,
                                    ClusterTask.ClusterRunning
                                )
                            )
                        }

                        return taskAwaitingWithOutput(context.flinkCluster, "")
                    }
                    else -> {
                        logger.warn("Cluster ${context.clusterId.name} requires a restart, but current status prevents from restarting the cluster")
                    }
                }
            } else if (changes.contains("BOOTSTRAP")) {
                logger.info("Detected changes in: ${changes.joinToString(separator = ",")}")

                val clusterStatus = Status.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Running -> {
                        logger.info("Cluster ${context.clusterId.name} requires to restart the job")

                        Status.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        Status.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        Status.setRuntimeDigest(context.flinkCluster, actualRuntimeDigest)
                        Status.setBootstrapDigest(context.flinkCluster, actualBootstrapDigest)

                        Status.appendTasks(context.flinkCluster,
                            listOf(
                                ClusterTask.UpdatingCluster,
                                ClusterTask.CancelJob,
                                ClusterTask.DeleteBootstrapJob,
                                ClusterTask.CreateBootstrapJob,
                                ClusterTask.StartJob,
                                ClusterTask.ClusterRunning
                            )
                        )

                        return taskAwaitingWithOutput(context.flinkCluster, "")
                    }
                    else -> {
                        logger.warn("Cluster ${context.clusterId.name} requires to restart the job, but current status prevents from restarting the job")
                    }
                }
            } else {
                // nothing changed
            }
        }

        val now = context.controller.currentTimeMillis()

        val elapsedTime = now - context.operatorTimestamp

        val nextTask = Status.getNextOperatorTask(context.flinkCluster)

        if (isBootstrapJobDefined(context.flinkCluster) && elapsedTime > 10000) {
            val attempts = Status.getTaskAttempts(context.flinkCluster)

            val clusterRunning = context.controller.isClusterRunning(context.clusterId)

            if (!clusterRunning.isCompleted()) {
                logger.warn("Cluster ${context.clusterId.name} doesn't have a running job...")
                Status.setTaskAttempts(context.flinkCluster, attempts + 1)

                if (nextTask == null && attempts >= 3) {
                    Status.setTaskAttempts(context.flinkCluster, 0)

                    return taskFailedWithOutput(context.flinkCluster, "")
                }
            } else {
                if (attempts > 0) {
                    Status.setTaskAttempts(context.flinkCluster, 0)
                }

                if (clusterRunning.output) {
                    logger.info("Job finished. Suspending cluster ${context.clusterId.name}...")

                    Status.appendTasks(context.flinkCluster,
                        listOf(
                            ClusterTask.StoppingCluster,
                            ClusterTask.TerminatePods,
                            ClusterTask.SuspendCluster,
                            ClusterTask.ClusterHalted
                        )
                    )

                    return taskAwaitingWithOutput(context.flinkCluster, "")
                }
            }
        }

        if (context.flinkCluster.spec.bootstrap != null && nextTask == null) {
            val savepointMode = Configuration.getSavepointMode(context.flinkCluster)

            val lastSavepointsTimestamp = Status.getSavepointTimestamp(context.flinkCluster)

            val savepointIntervalInSeconds = Configuration.getSavepointInterval(context.flinkCluster)

            if (savepointMode.toUpperCase() == "AUTOMATIC" && now - lastSavepointsTimestamp > savepointIntervalInSeconds * 1000L) {
                Status.appendTasks(context.flinkCluster,
                    listOf(
                        ClusterTask.CreatingSavepoint,
                        ClusterTask.TriggerSavepoint,
                        ClusterTask.ClusterRunning
                    )
                )

                return taskAwaitingWithOutput(context.flinkCluster, "")
            }
        }

        if (elapsedTime > 5000) {
            val taskmanagerStatefulset = context.resources.taskmanagerStatefulSets[context.clusterId]
            val actualTaskManagers = taskmanagerStatefulset?.status?.replicas ?: 0
            val desiredTaskManagers = context.flinkCluster.spec?.taskManagers ?: 1
            val currentTaskManagers = context.flinkCluster.status?.taskManagers ?: 1
            val currentTaskSlots = context.flinkCluster.status?.taskSlots ?: 1

            if (actualTaskManagers != desiredTaskManagers || currentTaskManagers != desiredTaskManagers) {
                val clusterScaling = ClusterScaling(taskManagers = desiredTaskManagers, taskSlots = currentTaskSlots)
                val result = context.controller.scaleCluster(context.clusterId, clusterScaling)
                if (result.isCompleted()) {
                    return taskAwaitingWithOutput(context.flinkCluster, "")
                }
            }
        }

        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}