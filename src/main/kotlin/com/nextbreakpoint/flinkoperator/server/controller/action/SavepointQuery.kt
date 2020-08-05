package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.SavepointRequest
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class SavepointQuery(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<SavepointRequest, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SavepointQuery::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: SavepointRequest): Result<String?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val requests = mapOf(params.jobId to params.triggerId)

            val savepointRequestsStatus = flinkClient.getSavepointRequestsStatus(address, requests)

            val requestsFailed = savepointRequestsStatus.filter { it.value.status == "FAILED" }

            if (requestsFailed.isNotEmpty()) {
                return Result(
                    ResultStatus.ERROR,
                    null
                )
            }

            val requestsInProgress = savepointRequestsStatus.filter { it.value.status == "IN_PROGRESS" }

            if (requestsInProgress.isNotEmpty()) {
                return Result(
                    ResultStatus.OK,
                    null
                )
            }

            val requestsCompleted = savepointRequestsStatus.filter { it.value.status == "COMPLETED" }

            if (requestsCompleted.isEmpty()) {
                return Result(
                    ResultStatus.ERROR,
                    null
                )
            }

            val savepointInfo = requestsCompleted[params.jobId]

            if (savepointInfo?.location != null) {
                return Result(
                    ResultStatus.OK,
                    savepointInfo.location
                )
            } else {
                return Result(
                    ResultStatus.ERROR,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't get savepoint status", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}