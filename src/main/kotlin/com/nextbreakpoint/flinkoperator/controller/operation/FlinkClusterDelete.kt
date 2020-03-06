package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
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

    override fun execute(clusterId: ClusterId, params: Void?): OperationResult<Void?> {
        try {
            val response = kubeClient.deleteFlinkCluster(clusterId)

            if (response.statusCode == 200) {
                logger.info("[name=${clusterId.name}] Custom object deleted")

                return OperationResult(
                    OperationStatus.COMPLETED,
                    null
                )
            } else {
                logger.error("[name=${clusterId.name}] Can't delete custom object")

                return OperationResult(
                    OperationStatus.FAILED,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't delete cluster resource", e)

            return OperationResult(
                OperationStatus.FAILED,
                null
            )
        }
    }
}