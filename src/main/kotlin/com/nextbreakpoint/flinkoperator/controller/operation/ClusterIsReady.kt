package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class ClusterIsReady(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<ClusterScaling, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterIsReady::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterScaling): OperationResult<Void?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val overview = flinkClient.getOverview(address)

            if (overview.slotsAvailable >= params.taskManagers * params.taskSlots && overview.taskmanagers >= params.taskManagers) {
                return OperationResult(
                    OperationStatus.COMPLETED,
                    null
                )
            } else {
                return OperationResult(
                    OperationStatus.RETRY,
                    null
                )
            }
        } catch (e : Exception) {
            logger.debug("[name=${clusterId.name}] Can't get overview")

            return OperationResult(
                OperationStatus.FAILED,
                null
            )
        }
    }
}