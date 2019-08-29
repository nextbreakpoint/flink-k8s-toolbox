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
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class SavepointCreate(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext, val cache: OperatorCache) : OperatorCommand<Void?, List<OperatorTask>>(flinkOptions, flinkContext, kubernetesContext) {
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

            val operatorStatus = OperatorAnnotations.getCurrentTaskStatus(flinkCluster)

            if (operatorStatus != TaskStatus.IDLE) {
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

            val clusterStatus = OperatorAnnotations.getClusterStatus(flinkCluster)

            if (clusterStatus != ClusterStatus.RUNNING) {
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

            val statusList = listOf(
                OperatorTask.CHECKPOINTING_CLUSTER,
                OperatorTask.CREATE_SAVEPOINT,
                OperatorTask.CLUSTER_RUNNING
            )

            OperatorAnnotations.appendTasks(flinkCluster, statusList)

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