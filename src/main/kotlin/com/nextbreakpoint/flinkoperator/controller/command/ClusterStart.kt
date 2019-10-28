package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class ClusterStart(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext, val cache: OperatorCache) : OperatorCommand<StartOptions, List<OperatorTask>>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(ClusterStart::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StartOptions): Result<List<OperatorTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val clusterStatus = OperatorAnnotations.getClusterStatus(flinkCluster)

            val operatorStatus = OperatorAnnotations.getCurrentTaskStatus(flinkCluster)

            if (operatorStatus == TaskStatus.IDLE) {
                val statusList = if (flinkCluster.spec?.flinkJob == null) {
                    when (clusterStatus) {
                        ClusterStatus.TERMINATED ->
                            listOf(
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.CREATE_RESOURCES,
                                OperatorTask.CLUSTER_RUNNING
                            )
                        ClusterStatus.SUSPENDED ->
                            listOf(
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.RESTART_PODS,
                                OperatorTask.CLUSTER_RUNNING
                            )
                        ClusterStatus.FAILED ->
                            listOf(
                                OperatorTask.STOPPING_CLUSTER,
                                OperatorTask.TERMINATE_PODS,
                                OperatorTask.DELETE_RESOURCES,
                                OperatorTask.STARTING_CLUSTER,
                                OperatorTask.CREATE_RESOURCES,
                                OperatorTask.CLUSTER_RUNNING
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
                                    OperatorTask.CLUSTER_RUNNING
                                )
                            } else {
                                listOf(
                                    OperatorTask.STARTING_CLUSTER,
                                    OperatorTask.CREATE_RESOURCES,
                                    OperatorTask.DELETE_UPLOAD_JOB,
                                    OperatorTask.UPLOAD_JAR,
                                    OperatorTask.START_JOB,
                                    OperatorTask.CLUSTER_RUNNING
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
                                    OperatorTask.CLUSTER_RUNNING
                                )
                            } else {
                                listOf(
                                    OperatorTask.STARTING_CLUSTER,
                                    OperatorTask.RESTART_PODS,
                                    OperatorTask.DELETE_UPLOAD_JOB,
                                    OperatorTask.UPLOAD_JAR,
                                    OperatorTask.START_JOB,
                                    OperatorTask.CLUSTER_RUNNING
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
                                    OperatorTask.CLUSTER_RUNNING
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
                                    OperatorTask.CLUSTER_RUNNING
                                )
                            }
                        else -> listOf()
                    }
                }

                if (statusList.isNotEmpty()) {
                    OperatorAnnotations.appendTasks(flinkCluster, statusList)
                    return Result(
                        ResultStatus.SUCCESS,
                        statusList
                    )
                } else {
                    logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                    return Result(
                        ResultStatus.AWAIT,
                        listOf(
                            OperatorAnnotations.getCurrentTask(
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
                        OperatorAnnotations.getCurrentTask(
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