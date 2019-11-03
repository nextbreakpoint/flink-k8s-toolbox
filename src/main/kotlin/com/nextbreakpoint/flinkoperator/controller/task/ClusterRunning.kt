package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorParameters
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import org.apache.log4j.Logger

class ClusterRunning : OperatorTaskHandler {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterRunning::class.simpleName)
    }

    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorState.setClusterStatus(context.flinkCluster, ClusterStatus.RUNNING)
        OperatorState.setOperatorTaskAttempts(context.flinkCluster, 0)
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
                    ClusterStatus.RUNNING -> {
                        logger.info("Cluster ${context.clusterId.name} requires a restart")

                        OperatorState.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorState.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorState.setFlinkImageDigest(context.flinkCluster, actualFlinkImageDigest)
                        OperatorState.setFlinkJobDigest(context.flinkCluster, actualFlinkJobDigest)

                        OperatorState.appendTasks(context.flinkCluster,
                            listOf(
                                OperatorTask.STOPPING_CLUSTER,
                                OperatorTask.CANCEL_JOB,
                                OperatorTask.TERMINATE_PODS,
                                OperatorTask.DELETE_RESOURCES,
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.DELETE_UPLOAD_JOB,
                                OperatorTask.CREATE_RESOURCES,
                                OperatorTask.UPLOAD_JAR,
                                OperatorTask.START_JOB,
                                OperatorTask.CLUSTER_RUNNING
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
                    ClusterStatus.RUNNING -> {
                        logger.info("Cluster ${context.clusterId.name} requires to restart the job")

                        OperatorState.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorState.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorState.setFlinkImageDigest(context.flinkCluster, actualFlinkImageDigest)
                        OperatorState.setFlinkJobDigest(context.flinkCluster, actualFlinkJobDigest)

                        OperatorState.appendTasks(context.flinkCluster,
                            listOf(
                                OperatorTask.STOPPING_CLUSTER,
                                OperatorTask.CANCEL_JOB,
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.DELETE_UPLOAD_JOB,
                                OperatorTask.UPLOAD_JAR,
                                OperatorTask.START_JOB,
                                OperatorTask.CLUSTER_RUNNING
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

        val elapsedTime = now - context.lastUpdated

        val nextTask = OperatorState.getNextOperatorTask(context.flinkCluster)

        if (context.flinkCluster.spec?.flinkJob != null && elapsedTime > 10000) {
            val attempts = OperatorState.getOperatorTaskAttempts(context.flinkCluster)

            val clusterRunning = context.controller.isClusterRunning(context.clusterId)

            if (clusterRunning.status != ResultStatus.SUCCESS) {
                logger.warn("Cluster ${context.clusterId.name} doesn't have a running job...")
                OperatorState.setOperatorTaskAttempts(context.flinkCluster, attempts + 1)

                if (nextTask == null && attempts >= 3) {
                    OperatorState.setOperatorTaskAttempts(context.flinkCluster, 0)

                    return Result(
                        ResultStatus.FAILED,
                        ""
                    )
                }
            } else {
                if (attempts > 0) {
                    OperatorState.setOperatorTaskAttempts(context.flinkCluster, 0)
                }

                if (clusterRunning.output) {
                    logger.info("Job finished. Suspending cluster ${context.clusterId.name}...")

                    OperatorState.appendTasks(context.flinkCluster,
                        listOf(
                            OperatorTask.STOPPING_CLUSTER,
                            OperatorTask.TERMINATE_PODS,
                            OperatorTask.SUSPEND_CLUSTER,
                            OperatorTask.CLUSTER_HALTED
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
                        OperatorTask.CHECKPOINTING_CLUSTER,
                        OperatorTask.CREATE_SAVEPOINT,
                        OperatorTask.CLUSTER_RUNNING
                    )
                )

                return Result(
                    ResultStatus.AWAIT,
                    ""
                )
            }
        }

//        val annotations = context.flinkCluster.metadata?.annotations.orEmpty()
//        if (annotations["flink-operator/action"]?.toUpperCase() == "SUSPEND") {
//            if (annotations["flink-operator/without-savepoint"]?.toUpperCase() == "TRUE") {
//                val options = StopOptions(withoutSavepoint = true, deleteResources = false)
//                context.controller.stopCluster(context.clusterId, options, context.cache)
//            } else {
//                val options = StopOptions(withoutSavepoint = false, deleteResources = false)
//                context.controller.stopCluster(context.clusterId, options, context.cache)
//            }
//        } else if (annotations["flink-operator/action"]?.toUpperCase() == "TERMINATED") {
//            if (annotations["flink-operator/without-savepoint"]?.toUpperCase() == "TRUE") {
//                val options = StopOptions(withoutSavepoint = true, deleteResources = true)
//                context.controller.stopCluster(context.clusterId, options, context.cache)
//            } else {
//                val options = StopOptions(withoutSavepoint = false, deleteResources = true)
//                context.controller.stopCluster(context.clusterId, options, context.cache)
//            }
//        }

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