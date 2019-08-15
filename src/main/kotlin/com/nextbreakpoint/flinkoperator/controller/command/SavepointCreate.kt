package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class SavepointCreate(flinkOptions: FlinkOptions, val cache: OperatorCache) : OperatorCommand<Void?, List<OperatorTask>>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(SavepointCreate::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<List<OperatorTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            val clusterStatus = OperatorAnnotations.getClusterStatus(flinkCluster)

            val operatorStatus = OperatorAnnotations.getCurrentOperatorStatus(flinkCluster)

            if (flinkCluster.spec?.flinkJob == null) {
                logger.info("Job not defined for cluster ${clusterId.name}")

                return Result(
                    ResultStatus.FAILED,
                    listOf()
                )
            }

            if (operatorStatus == TaskStatus.IDLE) {
                val statusList = when (clusterStatus) {
                    ClusterStatus.RUNNING ->
                        listOf(
                            OperatorTask.CHECKPOINTING_CLUSTER,
                            OperatorTask.CREATE_SAVEPOINT,
                            OperatorTask.CLUSTER_RUNNING
                        )
                    else -> listOf()
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