package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class ClusterIsReady(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<ClusterScaling, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterIsReady::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: ClusterScaling): OperationResult<Boolean> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val overview = flinkClient.getOverview(address)

            if (overview.slotsAvailable >= params.taskManagers * params.taskSlots && overview.taskmanagers >= params.taskManagers) {
                OperationResult(
                    OperationStatus.OK,
                    true
                )
            } else {
                OperationResult(
                    OperationStatus.OK,
                    false
                )
            }
        } catch (e : Exception) {
            logger.debug("[name=${clusterSelector.name}] Can't get overview")

            OperationResult(
                OperationStatus.ERROR,
                false
            )
        }
    }
}