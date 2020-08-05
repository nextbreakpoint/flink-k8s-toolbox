package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.SavepointRequest
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class SavepointTrigger(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<SavepointOptions, SavepointRequest?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SavepointTrigger::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: SavepointOptions): Result<SavepointRequest?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val runningJobs = flinkClient.listRunningJobs(address)

            if (runningJobs.isEmpty()) {
                logger.warn("[name=${clusterSelector.name}] Can't find a running job")

                return Result(
                    ResultStatus.ERROR,
                    null
                )
            }

            if (runningJobs.size > 1) {
                logger.warn("[name=${clusterSelector.name}] There are multiple jobs running")

                return Result(
                    ResultStatus.ERROR,
                    null
                )
            }

            val savepointRequests = flinkClient.triggerSavepoints(address, runningJobs, params.targetPath)

            return Result(
                ResultStatus.OK,
                savepointRequests.map {
                    SavepointRequest(
                        jobId = it.key,
                        triggerId = it.value
                    )
                }.first()
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't trigger savepoint for job", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}