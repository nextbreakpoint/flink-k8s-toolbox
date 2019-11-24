package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.Status
import org.apache.log4j.Logger

class SavepointForget(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val cache: Cache) : Operation<Void?, List<ClusterTask>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SavepointForget::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<List<ClusterTask>> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            if (flinkCluster.spec?.bootstrap == null) {
                logger.info("[name=${clusterId.name}] Job not defined")

                return Result(
                    ResultStatus.FAILED,
                    listOf()
                )
            }

            val operatorStatus = Status.getCurrentTaskStatus(flinkCluster)

            if (operatorStatus != TaskStatus.Idle) {
                logger.warn("[name=${clusterId.name}] Can't change tasks sequence")

                return Result(
                    ResultStatus.AWAIT,
                    listOf(
                        Status.getCurrentTask(
                            flinkCluster
                        )
                    )
                )
            }

            val clusterStatus = Status.getClusterStatus(flinkCluster)

            if (clusterStatus != ClusterStatus.Suspended && clusterStatus != ClusterStatus.Terminated) {
                logger.warn("[name=${clusterId.name}] Can't change tasks sequence")

                return Result(
                    ResultStatus.AWAIT,
                    listOf(
                        Status.getCurrentTask(
                            flinkCluster
                        )
                    )
                )
            }

            val statusList = listOf(
                ClusterTask.EraseSavepoint,
                ClusterTask.ClusterHalted
            )

            Status.appendTasks(flinkCluster, statusList)

            return Result(
                ResultStatus.SUCCESS,
                statusList
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't change tasks sequence", e)

            return Result(
                ResultStatus.FAILED,
                listOf()
            )
        }
    }
}