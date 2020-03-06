package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
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

    override fun execute(clusterId: ClusterId, params: V1Job): OperationResult<String?> {
        try {
            logger.info("[name=${clusterId.name}] Creating bootstrap job...")

            val jobOut = kubeClient.createBootstrapJob(clusterId, params)

            logger.info("[name=${clusterId.name}] Bootstrap job created: ${jobOut.metadata.name}")

            return OperationResult(
                OperationStatus.COMPLETED,
                jobOut.metadata.name
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't create bootstrap job", e)

            return OperationResult(
                OperationStatus.FAILED,
                null
            )
        }
    }
}