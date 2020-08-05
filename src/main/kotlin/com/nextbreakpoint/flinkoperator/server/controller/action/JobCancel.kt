package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkclient.model.JobIdWithStatus.StatusEnum
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

class JobCancel(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<SavepointOptions, SavepointRequest?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobCancel::class.simpleName)

        private val nonRunningSet = setOf(
            StatusEnum.SUSPENDING,
            StatusEnum.RESTARTING,
            StatusEnum.RECONCILING,
            StatusEnum.FAILING,
            StatusEnum.CREATED,
            StatusEnum.SUSPENDED
        )
    }

    override fun execute(clusterSelector: ClusterSelector, params: SavepointOptions): Result<SavepointRequest?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val allJobs = flinkClient.listJobs(address, setOf())

            val nonRunningJobs = allJobs
                .filter { nonRunningSet.contains(it.value) }
                .map { it.key }
                .toList()

            if (nonRunningJobs.isNotEmpty()) {
                nonRunningJobs.forEach {
                    logger.info("[name=${clusterSelector.name}] Stopping job $it...")
                }

                flinkClient.terminateJobs(address, nonRunningJobs)

                return Result(
                    ResultStatus.OK,
                    null
                )
            }

            val runningJobs = allJobs
                .filter { it.value == StatusEnum.RUNNING }
                .map { it.key }
                .toList()

            if (runningJobs.isEmpty()) {
                return Result(
                    ResultStatus.OK,
                    SavepointRequest(
                        jobId = "",
                        triggerId = ""
                    )
                )
            }

            if (runningJobs.size > 1) {
                logger.warn("[name=${clusterSelector.name}] There are multiple running jobs")

                runningJobs.forEach {
                    logger.info("[name=${clusterSelector.name}] Stopping job $it...")
                }

                flinkClient.terminateJobs(address, runningJobs)

                return Result(
                    ResultStatus.OK,
                    null
                )
            } else {
                // only one job is running at this point

                runningJobs.forEach {
                    logger.info("[name=${clusterSelector.name}] Cancelling job $it...")
                }

                val requests = flinkClient.cancelJobs(address, runningJobs, params.targetPath)

                if (requests.isEmpty()) {
                    flinkClient.terminateJobs(address, runningJobs)

                    return Result(
                        ResultStatus.OK,
                        null
                    )
                }

                return Result(
                    ResultStatus.OK,
                    requests.map {
                        SavepointRequest(
                            jobId = it.key,
                            triggerId = it.value
                        )
                    }.firstOrNull()
                )
            }
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't cancel job", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}