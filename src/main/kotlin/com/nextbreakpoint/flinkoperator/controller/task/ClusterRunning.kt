package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorParameters
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorTask
import org.apache.log4j.Logger

class ClusterRunning : OperatorTask {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterRunning::class.simpleName)
    }

    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorState.setClusterStatus(context.flinkCluster, ClusterStatus.Running)
        OperatorState.setTaskAttempts(context.flinkCluster, 0)
        OperatorState.appendTasks(context.flinkCluster, listOf())

        val taskManagers = context.flinkCluster.spec?.taskManagers ?: 0
        val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
        OperatorState.setTaskManagers(context.flinkCluster, taskManagers)
        OperatorState.setTaskSlots(context.flinkCluster, taskSlots)
        OperatorState.setJobParallelism(context.flinkCluster, taskManagers * taskSlots)

        val savepointPath = context.flinkCluster.spec?.operator?.savepointPath
        OperatorState.setSavepointPath(context.flinkCluster, savepointPath)

        OperatorState.updateSavepointTimestamp(context.flinkCluster)

        return Result(
            ResultStatus.SUCCESS,
            "Status of cluster ${context.clusterId.name} has been updated"
        )
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(
            ResultStatus.SUCCESS,
            "Cluster ${context.clusterId.name} is running..."
        )
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        val jobManagerDigest = OperatorState.getJobManagerDigest(context.flinkCluster)
        val taskManagerDigest = OperatorState.getTaskManagerDigest(context.flinkCluster)
        val flinkImageDigest = OperatorState.getRuntimeDigest(context.flinkCluster)
        val flinkJobDigest = OperatorState.getBootstrapDigest(context.flinkCluster)

        val manualAction = OperatorAnnotations.getManualAction(context.flinkCluster)
        if (manualAction == ManualAction.STOP) {
            val withoutSavepoint = OperatorAnnotations.isWithSavepoint(context.flinkCluster)
            val deleteResources = OperatorAnnotations.isDeleteResources(context.flinkCluster)
            val options = StopOptions(withoutSavepoint = withoutSavepoint, deleteResources = deleteResources)
            val result = context.controller.stopCluster(context.clusterId, options)
            if (result.status == ResultStatus.SUCCESS) {
                OperatorAnnotations.setManualAction(context.flinkCluster, ManualAction.NONE)
                return Result(
                    ResultStatus.AWAIT,
                    ""
                )
            }
        } else {
            OperatorAnnotations.setManualAction(context.flinkCluster, ManualAction.NONE)
        }

        if (jobManagerDigest == null || taskManagerDigest == null || flinkImageDigest == null || flinkJobDigest == null) {
            return Result(
                ResultStatus.FAILED,
                "Missing required annotations in cluster ${context.clusterId.name}"
            )
        } else {
            val actualJobManagerDigest = CustomResources.computeDigest(context.flinkCluster.spec?.jobManager)
            val actualTaskManagerDigest = CustomResources.computeDigest(context.flinkCluster.spec?.taskManager)
            val actualRuntimeDigest = CustomResources.computeDigest(context.flinkCluster.spec?.runtime)
            val actualBootstrapDigest = CustomResources.computeDigest(context.flinkCluster.spec?.bootstrap)

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

                val clusterStatus = OperatorState.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Running -> {
                        logger.info("Cluster ${context.clusterId.name} requires a restart")

                        OperatorState.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorState.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorState.setRuntimeDigest(context.flinkCluster, actualRuntimeDigest)
                        OperatorState.setBootstrapDigest(context.flinkCluster, actualBootstrapDigest)

                        if (java.lang.Boolean.getBoolean("disableReplaceStrategy")) {
                            OperatorState.appendTasks(context.flinkCluster,
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
                            OperatorState.appendTasks(context.flinkCluster,
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

                        return Result(
                            ResultStatus.AWAIT,
                            ""
                        )
                    }
                    else -> {
                        logger.warn("Cluster ${context.clusterId.name} requires a restart, but current status prevents from restarting the cluster")
                    }
                }
            } else if (changes.contains("BOOTSTRAP")) {
                logger.info("Detected changes in: ${changes.joinToString(separator = ",")}")

                val clusterStatus = OperatorState.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Running -> {
                        logger.info("Cluster ${context.clusterId.name} requires to restart the job")

                        OperatorState.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorState.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorState.setRuntimeDigest(context.flinkCluster, actualRuntimeDigest)
                        OperatorState.setBootstrapDigest(context.flinkCluster, actualBootstrapDigest)

                        OperatorState.appendTasks(context.flinkCluster,
                            listOf(
                                ClusterTask.UpdatingCluster,
                                ClusterTask.CancelJob,
                                ClusterTask.DeleteBootstrapJob,
                                ClusterTask.CreateBootstrapJob,
                                ClusterTask.StartJob,
                                ClusterTask.ClusterRunning
                            )
                        )

                        return Result(
                            ResultStatus.AWAIT,
                            ""
                        )
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

        val nextTask = OperatorState.getNextOperatorTask(context.flinkCluster)

        if (context.flinkCluster.spec?.bootstrap != null && elapsedTime > 10000) {
            val attempts = OperatorState.getTaskAttempts(context.flinkCluster)

            val clusterRunning = context.controller.isClusterRunning(context.clusterId)

            if (clusterRunning.status != ResultStatus.SUCCESS) {
                logger.warn("Cluster ${context.clusterId.name} doesn't have a running job...")
                OperatorState.setTaskAttempts(context.flinkCluster, attempts + 1)

                if (nextTask == null && attempts >= 3) {
                    OperatorState.setTaskAttempts(context.flinkCluster, 0)

                    return Result(
                        ResultStatus.FAILED,
                        ""
                    )
                }
            } else {
                if (attempts > 0) {
                    OperatorState.setTaskAttempts(context.flinkCluster, 0)
                }

                if (clusterRunning.output) {
                    logger.info("Job finished. Suspending cluster ${context.clusterId.name}...")

                    OperatorState.appendTasks(context.flinkCluster,
                        listOf(
                            ClusterTask.StoppingCluster,
                            ClusterTask.TerminatePods,
                            ClusterTask.SuspendCluster,
                            ClusterTask.ClusterHalted
                        )
                    )

                    return Result(
                        ResultStatus.AWAIT,
                        ""
                    )
                }
            }
        }

        if (context.flinkCluster.spec.bootstrap != null && nextTask == null) {
            val savepointMode = OperatorParameters.getSavepointMode(context.flinkCluster)

            val lastSavepointsTimestamp = OperatorState.getSavepointTimestamp(context.flinkCluster)

            val savepointIntervalInSeconds = OperatorParameters.getSavepointInterval(context.flinkCluster)

            if (savepointMode.toUpperCase() == "AUTOMATIC" && now - lastSavepointsTimestamp > savepointIntervalInSeconds * 1000L) {
                OperatorState.appendTasks(context.flinkCluster,
                    listOf(
                        ClusterTask.CreatingSavepoint,
                        ClusterTask.TriggerSavepoint,
                        ClusterTask.ClusterRunning
                    )
                )

                return Result(
                    ResultStatus.AWAIT,
                    ""
                )
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
                if (result.status == ResultStatus.SUCCESS) {
                    return Result(
                        ResultStatus.AWAIT,
                        ""
                    )
                }
            }
        }

        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }
}