package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import org.apache.log4j.Logger

class SavepointCreate(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext, private val cache: OperatorCache) : OperatorCommand<Void?, List<OperatorTask>>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(SavepointCreate::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<List<OperatorTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            if (flinkCluster.spec?.flinkJob == null) {
                logger.info("Job not defined for cluster ${clusterId.name}")

                return Result(
                    ResultStatus.FAILED,
                    listOf()
                )
            }

            val operatorStatus = OperatorState.getCurrentTaskStatus(flinkCluster)

            if (operatorStatus != TaskStatus.IDLE) {
                logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    listOf(
                        OperatorState.getCurrentTask(
                            flinkCluster
                        )
                    )
                )
            }

            val clusterStatus = OperatorState.getClusterStatus(flinkCluster)

            if (clusterStatus != ClusterStatus.RUNNING) {
                logger.warn("Can't change tasks sequence of cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    listOf(
                        OperatorState.getCurrentTask(
                            flinkCluster
                        )
                    )
                )
            }

            val statusList = listOf(
                OperatorTask.CREATING_SAVEPOINT,
                OperatorTask.CREATE_SAVEPOINT,
                OperatorTask.CLUSTER_RUNNING
            )

            OperatorState.appendTasks(flinkCluster, statusList)

            return Result(
                ResultStatus.SUCCESS,
                statusList
            )
        } catch (e : Exception) {
            logger.error("Can't change tasks sequence of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                listOf()
            )
        }
    }
}