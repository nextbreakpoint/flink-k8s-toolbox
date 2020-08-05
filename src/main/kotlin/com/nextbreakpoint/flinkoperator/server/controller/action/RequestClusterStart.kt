package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.ManualAction
import com.nextbreakpoint.flinkoperator.common.StartOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.ControllerContext
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class RequestClusterStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: ControllerContext) : Action<StartOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestClusterStart::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: StartOptions): Result<Void?> {
        return try {
            context.setWithoutSavepoint(params.withoutSavepoint)
            context.setManualAction(ManualAction.START)

            kubeClient.updateAnnotations(clusterSelector, context.getAnnotations())

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't start cluster", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}