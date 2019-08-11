package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.OperatorTask
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskStatus
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorCache
import com.nextbreakpoint.operator.OperatorCommand
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

                return Result(ResultStatus.FAILED, listOf())
            }

            if (operatorStatus == TaskStatus.IDLE) {
                val statusList = when (clusterStatus) {
                    ClusterStatus.RUNNING ->
                        listOf(
                            OperatorTask.CHECKPOINTING_CLUSTER,
                            OperatorTask.CREATE_SAVEPOINT,
                            OperatorTask.RUN_CLUSTER
                        )
                    else -> listOf()
                }

                if (statusList.isNotEmpty()) {
                    OperatorAnnotations.appendOperatorTasks(flinkCluster, statusList)
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