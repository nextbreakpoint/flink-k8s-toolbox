package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class RequestClusterScale(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<ScaleOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestClusterScale::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: ScaleOptions): OperationResult<Void?> {
        return try {
            kubeClient.rescaleCluster(clusterSelector, params.taskManagers)

            OperationResult(
                OperationStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't scale cluster", e)

            OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}