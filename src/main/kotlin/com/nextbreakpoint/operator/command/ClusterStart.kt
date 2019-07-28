package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.OperatorTask
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.StartOptions
import com.nextbreakpoint.common.model.TaskStatus
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorCache
import com.nextbreakpoint.operator.OperatorCommand
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

            val operatorTask = OperatorAnnotations.getCurrentOperatorTask(flinkCluster)

            if (operatorStatus == TaskStatus.IDLE) {
                val statusList = if (params.startOnlyCluster) {
                    when (clusterStatus) {
                        ClusterStatus.TERMINATED ->
                            listOf(
                                operatorTask,
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.CREATE_RESOURCES,
                                OperatorTask.DELETE_UPLOAD_JOB,
                                OperatorTask.UPLOAD_JAR,
                                OperatorTask.SUSPEND_CLUSTER,
                                OperatorTask.DO_NOTHING
                            )
                        ClusterStatus.SUSPENDED ->
                            listOf(
                                operatorTask,
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.DELETE_UPLOAD_JOB,
                                OperatorTask.UPLOAD_JAR,
                                OperatorTask.SUSPEND_CLUSTER,
                                OperatorTask.DO_NOTHING
                            )
                        else -> listOf()
                    }
                } else {
                    when (clusterStatus) {
                        ClusterStatus.TERMINATED ->
                            if (params.withoutSavepoint) {
                                listOf(
                                    operatorTask,
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
                                    operatorTask,
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
                                    operatorTask,
                                    OperatorTask.STARTING_CLUSTER,
                                    OperatorTask.DELETE_UPLOAD_JOB,
                                    OperatorTask.UPLOAD_JAR,
                                    OperatorTask.ERASE_SAVEPOINT,
                                    OperatorTask.START_JOB,
                                    OperatorTask.RUN_CLUSTER
                                )
                            } else {
                                listOf(
                                    operatorTask,
                                    OperatorTask.STARTING_CLUSTER,
                                    OperatorTask.DELETE_UPLOAD_JOB,
                                    OperatorTask.UPLOAD_JAR,
                                    OperatorTask.START_JOB,
                                    OperatorTask.RUN_CLUSTER
                                )
                            }
                        else -> listOf()
                    }
                }

                if (statusList.isNotEmpty()) {
                    OperatorAnnotations.resetOperatorTasks(flinkCluster, statusList)
                    Kubernetes.updateAnnotations(flinkCluster)
                    return Result(ResultStatus.SUCCESS, statusList)
                } else {
                    logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                    return Result(ResultStatus.AWAIT, OperatorAnnotations.getCurrentOperatorTasks(flinkCluster))
                }
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