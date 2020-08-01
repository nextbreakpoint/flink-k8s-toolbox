package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.DeleteOptions
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class ClusterDeletePods(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<DeleteOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterDeletePods::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: DeleteOptions): OperationResult<Void?> {
        return try {
            logger.debug("[name=${clusterSelector.name}] Deleting pods...")

            kubeClient.deletePods(clusterSelector, params)

            OperationResult(
                OperationStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't delete pods", e)

            OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}