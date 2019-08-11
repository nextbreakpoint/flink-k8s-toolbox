package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class ClusterStart(flinkOptions: FlinkOptions, val cache: OperatorCache) : OperatorCommand<StartOptions, List<OperatorTask>>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(ClusterStart::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StartOptions): Result<List<OperatorTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val clusterStatus = OperatorAnnotations.getClusterStatus(flinkCluster)

            val operatorStatus = OperatorAnnotations.getCurrentOperatorStatus(flinkCluster)

            if (operatorStatus == TaskStatus.IDLE) {
                val statusList = if (flinkCluster.spec?.flinkJob == null) {
                    when (clusterStatus) {
                        ClusterStatus.TERMINATED ->
                            listOf(
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.CREATE_RESOURCES,
                                OperatorTask.RUN_CLUSTER
                            )
                        ClusterStatus.SUSPENDED ->
                            listOf(
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.RESTART_PODS,
                                OperatorTask.RUN_CLUSTER
                            )
                        ClusterStatus.FAILED ->
                            listOf(
                                OperatorTask.STOPPING_CLUSTER,
                                OperatorTask.TERMINATE_PODS,
                                OperatorTask.DELETE_RESOURCES,
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.CREATE_RESOURCES,
                                OperatorTask.RUN_CLUSTER
                            )
                        else -> listOf()
                    }
                } else {
                    when (clusterStatus) {
                        ClusterStatus.TERMINATED ->
                            if (params.withoutSavepoint) {
                                listOf(
                                    OperatorTask.STARTING_CLUSTER,
                                    OperatorTask.CREATE_RESOURCES,
                                    OperatorTask.DELETE_UPLOAD_JOB,
                                    OperatorTask.UPLOAD_JAR,
                                    OperatorTask.ERASE_SAVEPOINT,
                                    OperatorTask.START_JOB,
                                    OperatorTask.RUN_CLUSTER
                                )
                            } else {
                                listOf(
                                    OperatorTask.STARTING_CLUSTER,
                                    OperatorTask.CREATE_RESOURCES,
                                    OperatorTask.DELETE_UPLOAD_JOB,
                                    OperatorTask.UPLOAD_JAR,
                                    OperatorTask.START_JOB,
                                    OperatorTask.RUN_CLUSTER
                                )
                            }
                        ClusterStatus.SUSPENDED ->
                            if (params.withoutSavepoint) {
                                listOf(
                                    OperatorTask.STARTING_CLUSTER,
                                    OperatorTask.RESTART_PODS,
                                    OperatorTask.DELETE_UPLOAD_JOB,
                                    OperatorTask.UPLOAD_JAR,
                                    OperatorTask.ERASE_SAVEPOINT,
                                    OperatorTask.START_JOB,
                                    OperatorTask.RUN_CLUSTER
                                )
                            } else {
                                listOf(
                                    OperatorTask.STARTING_CLUSTER,
                                    OperatorTask.RESTART_PODS,
                                    OperatorTask.DELETE_UPLOAD_JOB,
                                    OperatorTask.UPLOAD_JAR,
                                    OperatorTask.START_JOB,
                                    OperatorTask.RUN_CLUSTER
                                )
                            }
                        ClusterStatus.FAILED ->
                            if (params.withoutSavepoint) {
                                listOf(
                                    OperatorTask.STOPPING_CLUSTER,
                                    OperatorTask.DELETE_UPLOAD_JOB,
                                    OperatorTask.TERMINATE_PODS,
                                    OperatorTask.DELETE_RESOURCES,
                                    OperatorTask.STARTING_CLUSTER,
                                    OperatorTask.RESTART_PODS,
                                    OperatorTask.UPLOAD_JAR,
                                    OperatorTask.ERASE_SAVEPOINT,
                                    OperatorTask.START_JOB,
                                    OperatorTask.RUN_CLUSTER
                                )
                            } else {
                                listOf(
                                    OperatorTask.STOPPING_CLUSTER,
                                    OperatorTask.DELETE_UPLOAD_JOB,
                                    OperatorTask.TERMINATE_PODS,
                                    OperatorTask.DELETE_RESOURCES,
                                    OperatorTask.STARTING_CLUSTER,
                                    OperatorTask.RESTART_PODS,
                                    OperatorTask.UPLOAD_JAR,
                                    OperatorTask.START_JOB,
                                    OperatorTask.RUN_CLUSTER
                                )
                            }
                        else -> listOf()
                    }
                }

                if (statusList.isNotEmpty()) {
                    OperatorAnnotations.appendOperatorTasks(flinkCluster, statusList)
                    return Result(
                        ResultStatus.SUCCESS,
                        statusList
                    )
                } else {
                    logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                    return Result(
                        ResultStatus.AWAIT,
                        listOf(
                            OperatorAnnotations.getCurrentOperatorTask(
                                flinkCluster
                            )
                        )
                    )
                }
            } else {
                logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    listOf(
                        OperatorAnnotations.getCurrentOperatorTask(
                            flinkCluster
                        )
                    )
                )
            }
        } catch (e : Exception) {
            logger.error("Can't change tasks sequence of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                listOf()
            )
        }
    }
}