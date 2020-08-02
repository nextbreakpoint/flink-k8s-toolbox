package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.SupervisorContext
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class RequestClusterStop(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: SupervisorContext) : Operation<StopOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestClusterStop::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: StopOptions): OperationResult<Void?> {
        return try {
            context.setWithoutSavepoint(params.withoutSavepoint)
            context.setDeleteResources(params.deleteResources)
            context.setManualAction(ManualAction.STOP)

            kubeClient.updateAnnotations(clusterSelector, context.getAnnotations())

            OperationResult(
                OperationStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't stop cluster", e)

            OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}