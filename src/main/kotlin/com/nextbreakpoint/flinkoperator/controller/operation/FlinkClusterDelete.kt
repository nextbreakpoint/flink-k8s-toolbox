package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class FlinkClusterDelete(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(FlinkClusterDelete::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): OperationResult<Void?> {
        return try {
            val response = kubeClient.deleteFlinkCluster(clusterSelector)

            if (response.statusCode == 200) {
                logger.info("[name=${clusterSelector.name}] Custom object deleted")

                OperationResult(
                    OperationStatus.OK,
                    null
                )
            } else {
                logger.error("[name=${clusterSelector.name}] Can't delete custom object")

                OperationResult(
                    OperationStatus.ERROR,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't delete cluster resource", e)

            OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}