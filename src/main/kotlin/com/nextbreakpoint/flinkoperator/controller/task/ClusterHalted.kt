package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorParameters
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import org.apache.log4j.Logger

class ClusterHalted : OperatorTaskHandler {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterHalted::class.simpleName)
    }

    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorState.setOperatorTaskAttempts(context.flinkCluster, 0)

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
                logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

                val clusterStatus = OperatorState.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.SUSPENDED, ClusterStatus.FAILED -> {
                        logger.info("Cluster ${context.clusterId.name} requires a restart")

                        OperatorState.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorState.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorState.setFlinkImageDigest(context.flinkCluster, actualFlinkImageDigest)
                        OperatorState.setFlinkJobDigest(context.flinkCluster, actualFlinkJobDigest)

                        OperatorState.appendTasks(context.flinkCluster,
                            listOf(
                                OperatorTask.STOPPING_CLUSTER,
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
                logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

                val clusterStatus = OperatorState.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.SUSPENDED, ClusterStatus.FAILED -> {
                        logger.info("Cluster ${context.clusterId.name} requires to restart the job")

                        OperatorState.setJobManagerDigest(context.flinkCluster, actualJobManagerDigest)
                        OperatorState.setTaskManagerDigest(context.flinkCluster, actualTaskManagerDigest)
                        OperatorState.setFlinkImageDigest(context.flinkCluster, actualFlinkImageDigest)
                        OperatorState.setFlinkJobDigest(context.flinkCluster, actualFlinkJobDigest)

                        OperatorState.appendTasks(context.flinkCluster,
                            listOf(
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

            val elapsedTime = context.controller.currentTimeMillis() - context.lastUpdated

            if (context.flinkCluster.spec?.flinkJob != null && elapsedTime > 10000) {
                val clusterStatus = OperatorState.getClusterStatus(context.flinkCluster)

                when (clusterStatus) {
                    ClusterStatus.FAILED -> {
                        val nextTask = OperatorState.getNextOperatorTask(context.flinkCluster)

                        val attempts = OperatorState.getOperatorTaskAttempts(context.flinkCluster)

                        val clusterRunning = context.controller.isClusterRunning(context.clusterId)

                        if (clusterRunning.status == ResultStatus.SUCCESS) {
                            logger.info("Cluster ${context.clusterId.name} seems to be running...")

                            OperatorState.appendTasks(context.flinkCluster, listOf(
                                OperatorTask.CLUSTER_RUNNING
                            ))

                            return Result(
                                ResultStatus.AWAIT,
                                ""
                            )
                        } else {
                            val restartPolicy = OperatorParameters.getJobRestartPolicy(context.flinkCluster)

                            if (restartPolicy.toUpperCase() == "ONFAILURE") {
                                val clusterReady = context.controller.isClusterReady(context.clusterId)

                                if (clusterReady.status == ResultStatus.SUCCESS) {
                                    logger.info("Cluster ${context.clusterId.name} seems to be ready...")
                                    OperatorState.setOperatorTaskAttempts(context.flinkCluster, attempts + 1)

                                    if (nextTask == null && attempts >= 3) {
                                        logger.info("Restarting job of cluster ${context.clusterId.name}...")

                                        OperatorState.appendTasks(context.flinkCluster, listOf(
                                            OperatorTask.DELETE_UPLOAD_JOB,
                                            OperatorTask.UPLOAD_JAR,
                                            OperatorTask.START_JOB,
                                            OperatorTask.CLUSTER_RUNNING
                                        ))

                                        return Result(
                                            ResultStatus.AWAIT,
                                            ""
                                        )
                                    }
                                } else {
                                    if (attempts > 0) {
                                        OperatorState.setOperatorTaskAttempts(context.flinkCluster, 0)
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