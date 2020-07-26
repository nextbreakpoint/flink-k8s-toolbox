package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.CacheBridge
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class RequestClusterStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val bridge: CacheBridge) : Operation<StartOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestClusterStart::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: StartOptions): OperationResult<Void?> {
        return try {
            bridge.setWithoutSavepoint(params.withoutSavepoint)
            bridge.setManualAction(ManualAction.START)

            kubeClient.updateAnnotations(clusterSelector, bridge.getAnnotations())

            OperationResult(
                OperationStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't start cluster", e)

            OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}