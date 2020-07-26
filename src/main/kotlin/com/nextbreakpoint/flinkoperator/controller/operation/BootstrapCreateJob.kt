package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import io.kubernetes.client.models.V1Job
import org.apache.log4j.Logger

class BootstrapCreateJob(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<V1Job, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(BootstrapCreateJob::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: V1Job): OperationResult<String?> {
        return try {
            val jobOut = kubeClient.createBootstrapJob(clusterSelector, params)

            OperationResult(
                OperationStatus.OK,
                jobOut.metadata.name
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't create bootstrap job", e)

            OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}