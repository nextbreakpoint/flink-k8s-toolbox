package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class SavepointQuery(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<SavepointRequest, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SavepointQuery::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: SavepointRequest): OperationResult<String?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val requests = mapOf(params.jobId to params.triggerId)

            val savepointRequestsStatus = flinkClient.getSavepointRequestsStatus(address, requests)

            val requestsFailed = savepointRequestsStatus.filter { it.value.status == "FAILED" }

            if (requestsFailed.isNotEmpty()) {
                return OperationResult(
                    OperationStatus.ERROR,
                    null
                )
            }

            val requestsInProgress = savepointRequestsStatus.filter { it.value.status == "IN_PROGRESS" }

            if (requestsInProgress.isNotEmpty()) {
                return OperationResult(
                    OperationStatus.OK,
                    null
                )
            }

            val requestsCompleted = savepointRequestsStatus.filter { it.value.status == "COMPLETED" }

            val savepointInfo = requestsCompleted[params.jobId]

            if (savepointInfo?.location != null) {
                return OperationResult(
                    OperationStatus.OK,
                    savepointInfo.location
                )
            } else {
                return OperationResult(
                    OperationStatus.ERROR,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't get savepoint status", e)

            return OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}