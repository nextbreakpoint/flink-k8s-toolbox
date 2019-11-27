package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.CacheAdapter
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class SavepointForget(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val adapter: CacheAdapter) : Operation<Void?, List<ClusterTask>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SavepointForget::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): OperationResult<List<ClusterTask>> {
        try {
            val clusterStatus = adapter.getClusterStatus()

            if (clusterStatus != ClusterStatus.Suspended && clusterStatus != ClusterStatus.Terminated && clusterStatus != ClusterStatus.Failed) {
                logger.warn("[name=${clusterId.name}] Can't change tasks sequence")

                return OperationResult(
                    OperationStatus.RETRY,
                    listOf()
                )
            }

            val statusList = listOf(
                ClusterTask.EraseSavepoint,
                ClusterTask.ClusterHalted
            )

            adapter.appendTasks(statusList)

            return OperationResult(
                OperationStatus.COMPLETED,
                statusList
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't change tasks sequence", e)

            return OperationResult(
                OperationStatus.FAILED,
                listOf()
            )
        }
    }
}