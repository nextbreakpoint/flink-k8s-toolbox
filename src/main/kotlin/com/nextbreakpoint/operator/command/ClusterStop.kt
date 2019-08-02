package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.OperatorTask
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.StopOptions
import com.nextbreakpoint.common.model.TaskStatus
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorCache
import com.nextbreakpoint.operator.OperatorCommand
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
                val statusList = if (params.stopOnlyJob) {
                    when (clusterStatus) {
                        ClusterStatus.RUNNING ->
                            if (params.withSavepoint) {
                                listOf(
                                    OperatorTask.STOPPING_CLUSTER,
                                    OperatorTask.CANCEL_JOB,
                                    OperatorTask.TERMINATE_PODS,
                                    OperatorTask.SUSPEND_CLUSTER,
                                    OperatorTask.HALT_CLUSTER
                                )
                            } else {
                                listOf(
                                    OperatorTask.STOPPING_CLUSTER,
                                    OperatorTask.STOP_JOB,
                                    OperatorTask.TERMINATE_PODS,
                                    OperatorTask.SUSPEND_CLUSTER,
                                    OperatorTask.HALT_CLUSTER
                                )
                            }
                        else -> listOf()
                    }
                } else {
                    when (clusterStatus) {
                        ClusterStatus.RUNNING ->
                            if (params.withSavepoint) {
                                listOf(
                                    OperatorTask.STOPPING_CLUSTER,
                                    OperatorTask.CANCEL_JOB,
                                    OperatorTask.TERMINATE_PODS,
                                    OperatorTask.DELETE_RESOURCES,
                                    OperatorTask.TERMINATE_CLUSTER,
                                    OperatorTask.HALT_CLUSTER
                                )
                            } else {
                                listOf(
                                    OperatorTask.STOPPING_CLUSTER,
                                    OperatorTask.STOP_JOB,
                                    OperatorTask.TERMINATE_PODS,
                                    OperatorTask.DELETE_RESOURCES,
                                    OperatorTask.TERMINATE_CLUSTER,
                                    OperatorTask.HALT_CLUSTER
                                )
                            }
                        ClusterStatus.SUSPENDED ->
                            listOf(
                                OperatorTask.STOPPING_CLUSTER,
                                OperatorTask.TERMINATE_PODS,
                                OperatorTask.DELETE_RESOURCES,
                                OperatorTask.TERMINATE_CLUSTER,
                                OperatorTask.HALT_CLUSTER
                            )
                        ClusterStatus.FAILED ->
                            listOf(
                                OperatorTask.STOPPING_CLUSTER,
                                OperatorTask.TERMINATE_PODS,
                                OperatorTask.DELETE_RESOURCES,
                                OperatorTask.TERMINATE_CLUSTER,
                                OperatorTask.HALT_CLUSTER
                            )
                        else -> listOf()
                    }
                }

                if (statusList.isNotEmpty()) {
                    OperatorAnnotations.resetOperatorTasks(flinkCluster, statusList)
                    Kubernetes.updateAnnotations(flinkCluster)
                    return Result(ResultStatus.SUCCESS, statusList)
                } else {
                    logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                    return Result(ResultStatus.AWAIT, listOf(OperatorAnnotations.getCurrentOperatorTask(flinkCluster)))
                }
            } else {
                logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                return Result(ResultStatus.AWAIT, listOf(OperatorAnnotations.getCurrentOperatorTask(flinkCluster)))
            }
        } catch (e : Exception) {
            logger.error("Can't change tasks sequence of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, listOf())
        }
    }
}