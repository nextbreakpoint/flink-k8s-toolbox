package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class ClusterStop(flinkOptions: FlinkOptions, val cache: OperatorCache) : OperatorCommand<StopOptions, List<OperatorTask>>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(ClusterStop::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StopOptions): Result<List<OperatorTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val clusterStatus = OperatorAnnotations.getClusterStatus(flinkCluster)

            val operatorStatus = OperatorAnnotations.getCurrentOperatorStatus(flinkCluster)

            if (operatorStatus == TaskStatus.IDLE) {
                val statusList = if (params.deleteResources) {
                    when (clusterStatus) {
                        ClusterStatus.RUNNING ->
                            if (flinkCluster.spec?.flinkJob != null) {
                                if (params.withoutSavepoint) {
                                    listOf(
                                        OperatorTask.STOPPING_CLUSTER,
                                        OperatorTask.STOP_JOB,
                                        OperatorTask.TERMINATE_PODS,
                                        OperatorTask.DELETE_RESOURCES,
                                        OperatorTask.TERMINATE_CLUSTER,
                                        OperatorTask.CLUSTER_HALTED
                                    )
                                } else {
                                    listOf(
                                        OperatorTask.STOPPING_CLUSTER,
                                        OperatorTask.CANCEL_JOB,
                                        OperatorTask.TERMINATE_PODS,
                                        OperatorTask.DELETE_RESOURCES,
                                        OperatorTask.TERMINATE_CLUSTER,
                                        OperatorTask.CLUSTER_HALTED
                                    )
                                }
                            } else {
                                listOf(
                                    OperatorTask.STOPPING_CLUSTER,
                                    OperatorTask.TERMINATE_PODS,
                                    OperatorTask.DELETE_RESOURCES,
                                    OperatorTask.TERMINATE_CLUSTER,
                                    OperatorTask.CLUSTER_HALTED
                                )
                            }
                        ClusterStatus.SUSPENDED ->
                            listOf(
                                OperatorTask.STOPPING_CLUSTER,
                                OperatorTask.TERMINATE_PODS,
                                OperatorTask.DELETE_RESOURCES,
                                OperatorTask.TERMINATE_CLUSTER,
                                OperatorTask.CLUSTER_HALTED
                            )
                        ClusterStatus.FAILED ->
                            listOf(
                                OperatorTask.STOPPING_CLUSTER,
                                OperatorTask.TERMINATE_PODS,
                                OperatorTask.DELETE_RESOURCES,
                                OperatorTask.TERMINATE_CLUSTER,
                                OperatorTask.CLUSTER_HALTED
                            )
                        else -> listOf()
                    }
                } else {
                    when (clusterStatus) {
                        ClusterStatus.RUNNING ->
                            if (flinkCluster.spec?.flinkJob != null) {
                                if (params.withoutSavepoint) {
                                    listOf(
                                        OperatorTask.STOPPING_CLUSTER,
                                        OperatorTask.STOP_JOB,
                                        OperatorTask.TERMINATE_PODS,
                                        OperatorTask.SUSPEND_CLUSTER,
                                        OperatorTask.CLUSTER_HALTED
                                    )
                                } else {
                                    listOf(
                                        OperatorTask.STOPPING_CLUSTER,
                                        OperatorTask.CANCEL_JOB,
                                        OperatorTask.TERMINATE_PODS,
                                        OperatorTask.SUSPEND_CLUSTER,
                                        OperatorTask.CLUSTER_HALTED
                                    )
                                }
                            } else {
                                listOf(
                                    OperatorTask.STOPPING_CLUSTER,
                                    OperatorTask.TERMINATE_PODS,
                                    OperatorTask.SUSPEND_CLUSTER,
                                    OperatorTask.CLUSTER_HALTED
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