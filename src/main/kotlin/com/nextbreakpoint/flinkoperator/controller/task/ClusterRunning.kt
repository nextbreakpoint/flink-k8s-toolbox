package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorParameters
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import org.apache.log4j.Logger

class ClusterRunning : OperatorTaskHandler {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterRunning::class.simpleName)
    }

    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorState.setClusterStatus(context.flinkCluster, ClusterStatus.Running)
        OperatorState.setTaskAttempts(context.flinkCluster, 0)
        OperatorState.appendTasks(context.flinkCluster, listOf())

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
        val flinkImageDigest = OperatorState.getFlinkImageDigest(context.flinkCluster)
        val flinkJobDigest = OperatorState.getFlinkJobDigest(context.flinkCluster)

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
            val actualFlinkImageDigest = CustomResources.computeDigest(context.flinkCluster.spec?.flinkImage)
            val actualFlinkJobDigest = CustomResources.computeDigest(context.flinkCluster.spec?.flinkJob)

            val changes = mutableListOf<String>()

            if (jobManagerDigest != actualJobManagerDigest) {
                changes.add("JOB_MANAGER")
            }

            if (taskManagerDigest != actualTaskManagerDigest) {
                changes.add("TASK_MANAGER")
            }

            if (flinkImageDigest != actualFlinkImageDigest) {
                changes.add("FLINK_IMAGE")
            }

            if (flinkJobDigest != actualFlinkJobDigest) {
                changes.add("FLINK_JOB")
            }

            if (changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("FLINK_IMAGE")) {
                logger.info("Detected changes in: ${changes.joinToString(separator = ",")}")

                val clusterStatus = OperatorState.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Running -> {
                        logger.info("Cluster ${context.clusterId.name} requires a restart")

                        OperatorState.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorState.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorState.setFlinkImageDigest(context.flinkCluster, actualFlinkImageDigest)
                        OperatorState.setFlinkJobDigest(context.flinkCluster, actualFlinkJobDigest)

                        OperatorState.appendTasks(context.flinkCluster,
                            listOf(
                                OperatorTask.StoppingCluster,
                                OperatorTask.CancelJob,
                                OperatorTask.TerminatePods,
                                OperatorTask.DeleteResources,
                                OperatorTask.StartingCluster,
                                OperatorTask.DeleteUploadJob,
                                OperatorTask.CreateResources,
                                OperatorTask.CreateUploadJob,
                                OperatorTask.StartJob,
                                OperatorTask.ClusterRunning
                            )
                        )

                        return Result(
                            ResultStatus.AWAIT,
                            ""
                        )
                    }
                    else -> {
                        logger.warn("Cluster ${context.clusterId.name} requires a restart, but current status prevents from restarting the cluster")
                    }
                }
            } else if (changes.contains("FLINK_JOB")) {
                logger.info("Detected changes in: ${changes.joinToString(separator = ",")}")

                val clusterStatus = OperatorState.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Running -> {
                        logger.info("Cluster ${context.clusterId.name} requires to restart the job")

                        OperatorState.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorState.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorState.setFlinkImageDigest(context.flinkCluster, actualFlinkImageDigest)
                        OperatorState.setFlinkJobDigest(context.flinkCluster, actualFlinkJobDigest)

                        OperatorState.appendTasks(context.flinkCluster,
                            listOf(
                                OperatorTask.StoppingCluster,
                                OperatorTask.CancelJob,
                                OperatorTask.StartingCluster,
                                OperatorTask.DeleteUploadJob,
                                OperatorTask.CreateUploadJob,
                                OperatorTask.StartJob,
                                OperatorTask.ClusterRunning
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

        if (context.flinkCluster.spec?.flinkJob != null && elapsedTime > 10000) {
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
                            OperatorTask.StoppingCluster,
                            OperatorTask.TerminatePods,
                            OperatorTask.SuspendCluster,
                            OperatorTask.ClusterHalted
                        )
                    )

                    return Result(
                        ResultStatus.AWAIT,
                        ""
                    )
                }
            }
        }

        if (context.flinkCluster.spec.flinkJob != null && nextTask == null) {
            val savepointMode = OperatorParameters.getSavepointMode(context.flinkCluster)

            val lastSavepointsTimestamp = OperatorState.getSavepointTimestamp(context.flinkCluster)

            val savepointIntervalInSeconds = OperatorParameters.getSavepointInterval(context.flinkCluster)

            if (savepointMode.toUpperCase() == "AUTOMATIC" && now - lastSavepointsTimestamp > savepointIntervalInSeconds * 1000L) {
                OperatorState.appendTasks(context.flinkCluster,
                    listOf(
                        OperatorTask.CreatingSavepoint,
                        OperatorTask.TriggerSavepoint,
                        OperatorTask.ClusterRunning
                    )
                )

                return Result(
                    ResultStatus.AWAIT,
                    ""
                )
            }
        }

        val desiredTaskManagers = context.flinkCluster.spec?.taskManagers ?: 1
        val taskmanagerStatefulset = context.resources.taskmanagerStatefulSets[context.clusterId]
        val actualTaskManagers = taskmanagerStatefulset?.status?.replicas ?: 0

        if (actualTaskManagers != desiredTaskManagers) {
            val options = ScaleOptions(taskManagers = desiredTaskManagers)
            val result = context.controller.scaleCluster(context.clusterId, options)
            if (result.status == ResultStatus.SUCCESS) {
                val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
                val jobParallelism = desiredTaskManagers * taskSlots
                OperatorState.setJobParallelism(context.flinkCluster, jobParallelism)
                return Result(
                    ResultStatus.AWAIT,
                    ""
                )
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