package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.FlinkClusterSpecification
import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.OperatorTask
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorContext
import org.apache.log4j.Logger

class HaltCluster : TaskHandler {
    companion object {
        private val logger: Logger = Logger.getLogger(HaltCluster::class.simpleName)
    }

    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorAnnotations.setOperatorTaskAttempts(context.flinkCluster, 0)

        return Result(ResultStatus.SUCCESS, "There is nothing to do")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(ResultStatus.SUCCESS, "I'll wait for the next task")
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        val jobManagerDigest = OperatorAnnotations.getJobManagerDigest(context.flinkCluster)
        val taskManagerDigest = OperatorAnnotations.getTaskManagerDigest(context.flinkCluster)
        val flinkImageDigest = OperatorAnnotations.getFlinkImageDigest(context.flinkCluster)
        val flinkJobDigest = OperatorAnnotations.getFlinkJobDigest(context.flinkCluster)

        if (jobManagerDigest== null || taskManagerDigest == null || flinkImageDigest == null || flinkJobDigest == null) {
            return Result(ResultStatus.FAILED, "Missing required annotations in cluster ${context.clusterId.name}")
        } else {
            val actualJobManagerDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec?.jobManager)
            val actualTaskManagerDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec?.taskManager)
            val actualFlinkImageDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec?.flinkImage)
            val actualFlinkJobDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec?.flinkJob)

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
                logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

                val clusterStatus = OperatorAnnotations.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.SUSPENDED, ClusterStatus.FAILED -> {
                        logger.info("Cluster restart required")

                        OperatorAnnotations.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorAnnotations.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorAnnotations.setFlinkImageDigest(context.flinkCluster, actualFlinkImageDigest)
                        OperatorAnnotations.setFlinkJobDigest(context.flinkCluster, actualFlinkJobDigest)

                        OperatorAnnotations.appendOperatorTasks(context.flinkCluster,
                            listOf(
                                OperatorTask.STOPPING_CLUSTER,
                                OperatorTask.TERMINATE_PODS,
                                OperatorTask.DELETE_RESOURCES,
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.DELETE_UPLOAD_JOB,
                                OperatorTask.CREATE_RESOURCES,
                                OperatorTask.UPLOAD_JAR,
                                OperatorTask.START_JOB,
                                OperatorTask.RUN_CLUSTER
                            )
                        )

                        return Result(ResultStatus.AWAIT, "")
                    }
                    else -> {
                        logger.warn("Cluster restart required, but current status prevents from restarting the cluster")
                    }
                }
            } else if (changes.contains("FLINK_JOB")) {
                logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

                val clusterStatus = OperatorAnnotations.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.SUSPENDED, ClusterStatus.FAILED -> {
                        logger.info("Job restart required")

                        OperatorAnnotations.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorAnnotations.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorAnnotations.setFlinkImageDigest(context.flinkCluster, actualFlinkImageDigest)
                        OperatorAnnotations.setFlinkJobDigest(context.flinkCluster, actualFlinkJobDigest)

                        OperatorAnnotations.appendOperatorTasks(context.flinkCluster,
                            listOf(
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.DELETE_UPLOAD_JOB,
                                OperatorTask.UPLOAD_JAR,
                                OperatorTask.START_JOB,
                                OperatorTask.RUN_CLUSTER
                            )
                        )

                        return Result(ResultStatus.AWAIT, "")
                    }
                    else -> {
                        logger.warn("Job restart required, but current status prevents from restarting the cluster")
                    }
                }
            } else {
                // nothing changed
            }

            val elapsedTime = System.currentTimeMillis() - context.lastUpdated

            if (elapsedTime > 10000) {
                val clusterStatus = OperatorAnnotations.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.FAILED -> {
                        val result = context.controller.isClusterRunning(context.clusterId)

                        val errors = OperatorAnnotations.getOperatorTaskAttempts(context.flinkCluster)

                        val nextTask = OperatorAnnotations.getNextOperatorTask(context.flinkCluster)

                        if (result.status == ResultStatus.SUCCESS) {
                            logger.warn("Cluster ${context.clusterId.name} doesn't have the expected status")
                            OperatorAnnotations.setOperatorTaskAttempts(context.flinkCluster, errors + 1)
                        }

                        if (errors > 0 && result.status != ResultStatus.SUCCESS) {
                            OperatorAnnotations.setOperatorTaskAttempts(context.flinkCluster, 0)
                        }

                        if (nextTask == null && errors >= 3) {
                            OperatorAnnotations.appendOperatorTasks(context.flinkCluster, listOf(OperatorTask.RUN_CLUSTER))

                            return Result(ResultStatus.AWAIT, "")
                        }
                    }
                    else -> {}
                }
            }
        }

        return Result(ResultStatus.AWAIT, "")
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }
}