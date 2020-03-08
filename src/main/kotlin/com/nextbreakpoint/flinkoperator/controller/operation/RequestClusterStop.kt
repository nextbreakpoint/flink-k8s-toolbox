package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.CacheBridge
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class RequestClusterStop(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val bridge: CacheBridge) : Operation<StopOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestClusterStop::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StopOptions): OperationResult<Void?> {
        try {
            bridge.setWithoutSavepoint(params.withoutSavepoint)
            bridge.setDeleteResources(params.deleteResources)
            bridge.setManualAction(ManualAction.STOP)

            kubeClient.updateAnnotations(clusterId, bridge.getAnnotations())

            return OperationResult(
                OperationStatus.COMPLETED,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't stop cluster", e)

            return OperationResult(
                OperationStatus.FAILED,
                null
            )
        }
    }
}