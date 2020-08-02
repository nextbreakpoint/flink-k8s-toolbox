package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import io.kubernetes.client.models.V1Deployment
import org.apache.log4j.Logger

class SupervisorCreateDeployment(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<V1Deployment, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SupervisorCreateDeployment::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: V1Deployment): OperationResult<String?> {
        return try {
            val jobOut = kubeClient.createSupervisorDeployment(clusterSelector, params)

            OperationResult(
                OperationStatus.OK,
                jobOut.metadata.name
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't create supervisor deployment", e)

            OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}