package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class JobCancel(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<SavepointOptions, SavepointRequest>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobCancel::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: SavepointOptions): OperationResult<SavepointRequest> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkClient.listRunningJobs(address)

            if (runningJobs.size != 1) {
                logger.warn("[name=${clusterId.name}] Expected exactly one job running")

                return OperationResult(
                    OperationStatus.FAILED,
                    SavepointRequest("", "")
                )
            }

            val inprogressCheckpoints = flinkClient.getCheckpointingStatistics(address, runningJobs)

            if (inprogressCheckpoints.filter { it.value.counts.inProgress > 0 }.isNotEmpty()) {
                logger.warn("[name=${clusterId.name}] Savepoint already in progress")

                return OperationResult(
                    OperationStatus.FAILED,
                    SavepointRequest("", "")
                )
            }

            val requests = runningJobs.map {
                logger.info("[name=${clusterId.name}] Cancelling job $it...")

                val response = flinkClient.createSavepoint(address, it, params.targetPath)

                it to response.requestId
            }.onEach {
                logger.info("[name=${clusterId.name}] Created savepoint request ${it.second} for job ${it.first}")
            }.toMap()

            return OperationResult(
                OperationStatus.COMPLETED,
                requests.map {
                    SavepointRequest(
                        jobId = it.key,
                        triggerId = it.value
                    )
                }.first()
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't trigger savepoint", e)

            return OperationResult(
                OperationStatus.FAILED,
                SavepointRequest("", "")
            )
        }
    }
}