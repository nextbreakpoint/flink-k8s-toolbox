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
        val jobManagerDigest = Status.getJobManagerDigest(context.flinkCluster)
        val taskManagerDigest = Status.getTaskManagerDigest(context.flinkCluster)
        val flinkImageDigest = Status.getRuntimeDigest(context.flinkCluster)
        val flinkJobDigest = Status.getBootstrapDigest(context.flinkCluster)

        val manualAction = Annotations.getManualAction(context.flinkCluster)
        if (manualAction == ManualAction.START) {
            val withoutSavepoint = Annotations.isWithSavepoint(context.flinkCluster)
            val options = StartOptions(withoutSavepoint = withoutSavepoint)
            val result = context.startCluster(context.clusterId, options)
            if (result.isCompleted()) {
                Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
                return taskAwaitingWithOutput(context.flinkCluster, "Starting cluster...")
            }
        } else {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
        }

        if (jobManagerDigest == null || taskManagerDigest == null || flinkImageDigest == null || flinkJobDigest == null) {
            return taskFailedWithOutput(context.flinkCluster, "Missing required digests")
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
                logger.info("[name=${context.flinkCluster.metadata.name}] Detected changes: ${changes.joinToString(separator = ",")}")

                val clusterStatus = Status.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Suspended, ClusterStatus.Failed -> {
                        Status.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        Status.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        Status.setRuntimeDigest(context.flinkCluster, actualRuntimeDigest)
                        Status.setBootstrapDigest(context.flinkCluster, actualBootstrapDigest)

                        if (java.lang.Boolean.getBoolean("disableReplaceStrategy")) {
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

                        return taskAwaitingWithOutput(context.flinkCluster, "Cluster requires a restart")
                    }
                    else -> {
                        logger.warn("[name=${context.flinkCluster.metadata.name}] Cluster requires a restart, but current status prevents from restarting the cluster")
                    }
                }
            } else if (changes.contains("BOOTSTRAP")) {
                val clusterStatus = Status.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Suspended, ClusterStatus.Failed -> {
                        logger.info("[name=${context.flinkCluster.metadata.name}] Cluster requires to restart the job")

                        Status.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        Status.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        Status.setRuntimeDigest(context.flinkCluster, actualRuntimeDigest)
                        Status.setBootstrapDigest(context.flinkCluster, actualBootstrapDigest)

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

                        return taskAwaitingWithOutput(context.flinkCluster, "Cluster requires to restart the job")
                    }
                    else -> {
                        logger.warn("[name=${context.flinkCluster.metadata.name}] Cluster requires to restart the job, but current status prevents from restarting the job")
                    }
                }
            } else {
                // nothing changed
            }

            val seconds = context.timeSinceLastUpdateInSeconds()

            if (isBootstrapJobDefined(context.flinkCluster) && seconds > 10) {
                val clusterStatus = Status.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Failed -> {
                        val nextTask = Status.getNextOperatorTask(context.flinkCluster)

                        val attempts = Status.getTaskAttempts(context.flinkCluster)

                        val clusterRunning = context.isClusterRunning(context.clusterId)

                        if (clusterRunning.isCompleted()) {
                            Status.appendTasks(
                                context.flinkCluster, listOf(
                                    ClusterTask.ClusterRunning
                                )
                            )

                            return taskAwaitingWithOutput(context.flinkCluster, "Cluster seems to be running...")
                        } else {
                            val restartPolicy = Configuration.getJobRestartPolicy(context.flinkCluster)

                            if (restartPolicy.toUpperCase() == "ALWAYS") {
                                val clusterScaling = ClusterScaling(
                                    taskManagers = context.flinkCluster.status.taskManagers,
                                    taskSlots = context.flinkCluster.status.taskSlots
                                )

                                val clusterReady = context.isClusterReady(context.clusterId, clusterScaling)

                                if (clusterReady.isCompleted()) {
                                    logger.info("[name=${context.flinkCluster.metadata.name}] Cluster seems to be ready...")
                                    Status.setTaskAttempts(context.flinkCluster, attempts + 1)

                                    if (nextTask == null && attempts >= 3) {
                                        Status.appendTasks(
                                            context.flinkCluster, listOf(
                                                ClusterTask.DeleteBootstrapJob,
                                                ClusterTask.CreateBootstrapJob,
                                                ClusterTask.StartJob,
                                                ClusterTask.ClusterRunning
                                            )
                                        )

                                        return taskAwaitingWithOutput(context.flinkCluster, "Cluster is ready")
                                    }
                                } else {
                                    if (attempts > 0) {
                                        Status.setTaskAttempts(context.flinkCluster, 0)
                                    }
                                }
                            }
                        }
                    }
                    else -> {
                    }
                }
            }
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Cluster halted")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}