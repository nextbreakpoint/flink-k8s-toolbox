package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.OperatorTask
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.StopOptions
import com.nextbreakpoint.operator.OperatorCache
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class ClusterStop(flinkOptions: FlinkOptions, val cache: OperatorCache) : OperatorCommand<StopOptions, List<OperatorTask>>(flinkOptions) {
    private val logger = Logger.getLogger(ClusterStop::class.simpleName)

    override fun execute(clusterId: ClusterId, params: StopOptions): Result<List<OperatorTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val clusterStatus = OperatorAnnotations.getClusterStatus(flinkCluster)

            val operatorTask = OperatorAnnotations.getCurrentOperatorTask(flinkCluster)

            val statusList = if (params.stopOnlyJob) {
                when (clusterStatus) {
                    ClusterStatus.RUNNING ->
                        if (params.withSavepoint) {
                            listOf(
                                operatorTask,
                                OperatorTask.CANCEL_JOB,
                                OperatorTask.SUSPEND_CLUSTER,
                                OperatorTask.DO_NOTHING
                            )
                        } else {
                            listOf(
                                operatorTask,
                                OperatorTask.STOP_JOB,
                                OperatorTask.SUSPEND_CLUSTER,
                                OperatorTask.DO_NOTHING
                            )
                        }
                    else -> listOf()
                }
            } else {
                when (clusterStatus) {
                    ClusterStatus.RUNNING ->
                        if (params.withSavepoint) {
                            listOf(
                                operatorTask,
                                OperatorTask.CANCEL_JOB,
                                OperatorTask.TERMINATE_PODS,
                                OperatorTask.DELETE_RESOURCES,
                                OperatorTask.TERMINATE_CLUSTER,
                                OperatorTask.DO_NOTHING
                            )
                        } else {
                            listOf(
                                operatorTask,
                                OperatorTask.STOP_JOB,
                                OperatorTask.TERMINATE_PODS,
                                OperatorTask.DELETE_RESOURCES,
                                OperatorTask.TERMINATE_CLUSTER,
                                OperatorTask.DO_NOTHING
                            )
                        }
                    ClusterStatus.SUSPENDED ->
                        listOf(
                            operatorTask,
                            OperatorTask.TERMINATE_PODS,
                            OperatorTask.DELETE_RESOURCES,
                            OperatorTask.TERMINATE_CLUSTER,
                            OperatorTask.DO_NOTHING
                        )
                    ClusterStatus.FAILED ->
                        listOf(
                            operatorTask,
                            OperatorTask.TERMINATE_PODS,
                            OperatorTask.DELETE_RESOURCES,
                            OperatorTask.TERMINATE_CLUSTER,
                            OperatorTask.DO_NOTHING
                        )
                    else -> listOf()
                }
            }

            if (statusList.isNotEmpty()) {
                OperatorAnnotations.setClusterStatus(flinkCluster, ClusterStatus.STOPPING)
                OperatorAnnotations.resetOperatorTasks(flinkCluster, statusList)
                Kubernetes.updateAnnotations(flinkCluster)
                return Result(ResultStatus.SUCCESS, statusList)
            } else {
                logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                return Result(ResultStatus.AWAIT, OperatorAnnotations.getCurrentOperatorTasks(flinkCluster))
            }
        } catch (e : Exception) {
            logger.error("Can't set tasks sequence of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, listOf())
        }
    }
}