package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobAction
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class SavepointQuery(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: JobContext) : JobAction<SavepointRequest, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SavepointQuery::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, jobName: String, params: SavepointRequest): Result<String?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

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
            logger.error("Can't get savepoint status (${context.getJobId()})", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}