package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import io.kubernetes.client.models.V1Service
import org.apache.log4j.Logger

class ClusterCreateService(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<V1Service, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreateService::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: V1Service): OperationResult<String?> {
        try {
            val serviceOut = kubeClient.createService(clusterId, params)

            return OperationResult(
                OperationStatus.COMPLETED,
                serviceOut.metadata.name
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't create resources", e)

            return OperationResult(
                OperationStatus.FAILED,
                null
            )
        }
    }
}