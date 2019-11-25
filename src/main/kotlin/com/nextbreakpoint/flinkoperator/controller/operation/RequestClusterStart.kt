package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.CacheAdapter
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class RequestClusterStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val adapter: CacheAdapter) : Operation<StartOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestClusterStart::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StartOptions): Result<Void?> {
        try {
            adapter.setWithoutSavepoint(params.withoutSavepoint)
            adapter.setManualAction(ManualAction.START)

            kubeClient.updateAnnotations(clusterId, adapter.getAnnotations())

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't start cluster", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}