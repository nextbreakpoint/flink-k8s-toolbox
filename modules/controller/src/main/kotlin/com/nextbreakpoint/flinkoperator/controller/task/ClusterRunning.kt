package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorParameters
import com.nextbreakpoint.flinkoperator.common.utils.CustomResourceUtils
import org.apache.log4j.Logger

class ClusterRunning : OperatorTaskHandler {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterRunning::class.simpleName)
    }

    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorAnnotations.setClusterStatus(context.flinkCluster, ClusterStatus.RUNNING)
        OperatorAnnotations.setOperatorTaskAttempts(context.flinkCluster, 0)
        OperatorAnnotations.appendOperatorTasks(context.flinkCluster, listOf())

        OperatorAnnotations.updateSavepointTimestamp(context.flinkCluster)

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
        val jobManagerDigest = OperatorAnnotations.getJobManagerDigest(context.flinkCluster)
        val taskManagerDigest = OperatorAnnotations.getTaskManagerDigest(context.flinkCluster)
        val flinkImageDigest = OperatorAnnotations.getFlinkImageDigest(context.flinkCluster)
        val flinkJobDigest = OperatorAnnotations.getFlinkJobDigest(context.flinkCluster)

        if (jobManagerDigest== null || taskManagerDigest == null || flinkImageDigest == null || flinkJobDigest == null) {
            return Result(
                ResultStatus.FAILED,
                "Missing required annotations in cluster ${context.clusterId.name}"
            )
        } else {
            val actualJobManagerDigest = CustomResourceUtils.computeDigest(context.flinkCluster.spec?.jobManager)
            val actualTaskManagerDigest = CustomResourceUtils.computeDigest(context.flinkCluster.spec?.taskManager)
            val actualFlinkImageDigest = CustomResourceUtils.computeDigest(context.flinkCluster.spec?.flinkImage)
            val actualFlinkJobDigest = CustomResourceUtils.computeDigest(context.flinkCluster.spec?.flinkJob)

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

                val clusterStatus = OperatorAnnotations.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.RUNNING -> {
                        logger.info("Cluster restart required")

                        OperatorAnnotations.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorAnnotations.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorAnnotations.setFlinkImageDigest(context.flinkCluster, actualFlinkImageDigest)
                        OperatorAnnotations.setFlinkJobDigest(context.flinkCluster, actualFlinkJobDigest)

                        OperatorAnnotations.appendOperatorTasks(context.flinkCluster,
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
                        logger.warn("Cluster restart required, but current status prevents from restarting the cluster")
                    }
                }
            } else if (changes.contains("FLINK_JOB")) {
                logger.info("Detected changes in: ${changes.joinToString(separator = ",")}")

                val clusterStatus = OperatorAnnotations.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.RUNNING -> {
                        logger.info("Job restart required")

                        OperatorAnnotations.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorAnnotations.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorAnnotations.setFlinkImageDigest(context.flinkCluster, actualFlinkImageDigest)
                        OperatorAnnotations.setFlinkJobDigest(context.flinkCluster, actualFlinkJobDigest)

                        OperatorAnnotations.appendOperatorTasks(context.flinkCluster,
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
                        logger.warn("Job restart required, but current status prevents from restarting the cluster")
                    }
                }
            } else {
                // nothing changed
            }
        }

        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        val nextTask = OperatorAnnotations.getNextOperatorTask(context.flinkCluster)

        if (context.flinkCluster.spec?.flinkJob != null && elapsedTime > 10000) {
            val result = context.controller.isClusterRunning(context.clusterId)

            val errors = OperatorAnnotations.getOperatorTaskAttempts(context.flinkCluster)

            if (result.status != ResultStatus.SUCCESS) {
                logger.warn("Cluster ${context.clusterId.name} doesn't have the expected status")
                OperatorAnnotations.setOperatorTaskAttempts(context.flinkCluster, errors + 1)
            }

            if (errors > 0 && result.status == ResultStatus.SUCCESS) {
                OperatorAnnotations.setOperatorTaskAttempts(context.flinkCluster, 0)
            }

            if (nextTask == null && errors >= 3) {
                OperatorAnnotations.setOperatorTaskAttempts(context.flinkCluster, 0)

                return Result(
                    ResultStatus.FAILED,
                    ""
                )
            }

            if (result.status == ResultStatus.SUCCESS && result.output) {
                OperatorAnnotations.appendOperatorTasks(context.flinkCluster,
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

        if (context.flinkCluster.spec.flinkJob != null && nextTask == null) {
            val savepointMode = OperatorParameters.getSavepointMode(context.flinkCluster)

            val lastSavepointsTimestamp = OperatorAnnotations.getSavepointTimestamp(context.flinkCluster)

            val savepointIntervalInSeconds = OperatorParameters.getSavepointInterval(context.flinkCluster)

            if (savepointMode == "AUTOMATIC" && System.currentTimeMillis() - lastSavepointsTimestamp > savepointIntervalInSeconds * 1000L) {
                OperatorAnnotations.appendOperatorTasks(context.flinkCluster,
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