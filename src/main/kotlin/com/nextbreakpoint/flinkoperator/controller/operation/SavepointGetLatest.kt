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

class SavepointGetLatest(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<SavepointRequest, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SavepointGetLatest::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: SavepointRequest): OperationResult<String> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val requests = mapOf(params.jobId to params.triggerId)

            val pendingSavepointRequests = flinkClient.getPendingSavepointRequests(address, requests)

            if (pendingSavepointRequests.isNotEmpty()) {
                return OperationResult(
                    OperationStatus.ERROR,
                    ""
                )
            }

            val savepointPaths = flinkClient.getLatestSavepointPaths(address, requests)

            if (savepointPaths.isEmpty()) {
                logger.error("[name=${clusterSelector.name}] Can't find any savepoint")

                return OperationResult(
                    OperationStatus.ERROR,
                    ""
                )
            }

            return OperationResult(
                OperationStatus.OK,
                savepointPaths.values.first()
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't get savepoint status", e)

            return OperationResult(
                OperationStatus.ERROR,
                ""
            )
        }
    }
}