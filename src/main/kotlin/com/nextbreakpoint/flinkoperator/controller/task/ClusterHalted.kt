package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorParameters
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import org.apache.log4j.Logger

class ClusterHalted : OperatorTaskHandler {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterHalted::class.simpleName)
    }

    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorState.setTaskAttempts(context.flinkCluster, 0)

        return Result(
            ResultStatus.SUCCESS,
            "Nothing to do for cluster ${context.clusterId.name}"
        )
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(
            ResultStatus.SUCCESS,
            "Cluster ${context.clusterId.name} is idle..."
        )
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        val jobManagerDigest = OperatorState.getJobManagerDigest(context.flinkCluster)
        val taskManagerDigest = OperatorState.getTaskManagerDigest(context.flinkCluster)
        val flinkImageDigest = OperatorState.getRuntimeDigest(context.flinkCluster)
        val flinkJobDigest = OperatorState.getBootstrapDigest(context.flinkCluster)

        val manualAction = OperatorAnnotations.getManualAction(context.flinkCluster)
        if (manualAction == ManualAction.START) {
            val withoutSavepoint = OperatorAnnotations.isWithSavepoint(context.flinkCluster)
            val options = StartOptions(withoutSavepoint = withoutSavepoint)
            val result = context.controller.startCluster(context.clusterId, options)
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
                logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

                val clusterStatus = OperatorState.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Suspended, ClusterStatus.Failed -> {
                        logger.info("Cluster ${context.clusterId.name} requires a restart")

                        OperatorState.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorState.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorState.setRuntimeDigest(context.flinkCluster, actualRuntimeDigest)
                        OperatorState.setBootstrapDigest(context.flinkCluster, actualBootstrapDigest)

                        if (java.lang.Boolean.getBoolean("enableReplaceStrategy")) {
                            OperatorState.appendTasks(
                                context.flinkCluster,
                                listOf(
                                    OperatorTask.UpdatingCluster,
                                    OperatorTask.ReplaceResources,
                                    OperatorTask.DeleteBootstrapJob,
                                    OperatorTask.CreateBootstrapJob,
                                    OperatorTask.StartJob,
                                    OperatorTask.ClusterRunning
                                )
                            )
                        } else {
                            OperatorState.appendTasks(
                                context.flinkCluster,
                                listOf(
                                    OperatorTask.StoppingCluster,
                                    OperatorTask.TerminatePods,
                                    OperatorTask.DeleteResources,
                                    OperatorTask.StartingCluster,
                                    OperatorTask.CreateResources,
                                    OperatorTask.DeleteBootstrapJob,
                                    OperatorTask.CreateBootstrapJob,
                                    OperatorTask.StartJob,
                                    OperatorTask.ClusterRunning
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
                logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

                val clusterStatus = OperatorState.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Suspended, ClusterStatus.Failed -> {
                        logger.info("Cluster ${context.clusterId.name} requires to restart the job")

                        OperatorState.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorState.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorState.setRuntimeDigest(context.flinkCluster, actualRuntimeDigest)
                        OperatorState.setBootstrapDigest(context.flinkCluster, actualBootstrapDigest)

                        OperatorState.appendTasks(context.flinkCluster,
                            listOf(
                                OperatorTask.UpdatingCluster,
                                OperatorTask.DeleteBootstrapJob,
                                OperatorTask.CreateBootstrapJob,
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

            val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

            if (context.flinkCluster.spec?.bootstrap != null && elapsedTime > 10000) {
                val clusterStatus = OperatorState.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.Failed -> {
                        val nextTask = OperatorState.getNextOperatorTask(context.flinkCluster)

                        val attempts = OperatorState.getTaskAttempts(context.flinkCluster)

                        val clusterRunning = context.controller.isClusterRunning(context.clusterId)

                        if (clusterRunning.status == ResultStatus.SUCCESS) {
                            logger.info("Cluster ${context.clusterId.name} seems to be running...")

                            OperatorState.appendTasks(context.flinkCluster, listOf(
                                OperatorTask.ClusterRunning
                            ))

                            return Result(
                                ResultStatus.AWAIT,
                                ""
                            )
                        } else {
                            val restartPolicy = OperatorParameters.getJobRestartPolicy(context.flinkCluster)

                            if (restartPolicy.toUpperCase() == "ALWAYS") {
                                val clusterReady = context.controller.isClusterReady(context.clusterId)

                                if (clusterReady.status == ResultStatus.SUCCESS) {
                                    logger.info("Cluster ${context.clusterId.name} seems to be ready...")
                                    OperatorState.setTaskAttempts(context.flinkCluster, attempts + 1)

                                    if (nextTask == null && attempts >= 3) {
                                        logger.info("Restarting job of cluster ${context.clusterId.name}...")

                                        OperatorState.appendTasks(context.flinkCluster, listOf(
                                            OperatorTask.DeleteBootstrapJob,
                                            OperatorTask.CreateBootstrapJob,
                                            OperatorTask.StartJob,
                                            OperatorTask.ClusterRunning
                                        ))

                                        return Result(
                                            ResultStatus.AWAIT,
                                            ""
                                        )
                                    }
                                } else {
                                    if (attempts > 0) {
                                        OperatorState.setTaskAttempts(context.flinkCluster, 0)
                                    }
                                }
                            }
                        }
                    }
                    else -> {}
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