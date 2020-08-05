package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkclient.model.JobIdWithStatus
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class JobStop(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobStop::class.simpleName)

        private val nonRunningSet = setOf(
            JobIdWithStatus.StatusEnum.SUSPENDING,
            JobIdWithStatus.StatusEnum.RESTARTING,
            JobIdWithStatus.StatusEnum.RECONCILING,
            JobIdWithStatus.StatusEnum.FAILING,
            JobIdWithStatus.StatusEnum.CREATED,
            JobIdWithStatus.StatusEnum.SUSPENDED
        )
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): Result<Boolean> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val allJobs = flinkClient.listJobs(address, setOf())

            val nonRunningJobs = allJobs
                .filter { nonRunningSet.contains(it.value) }
                .map { it.key }
                .toList()

            nonRunningJobs.forEach {
                logger.info("[name=${clusterSelector.name}] Stopping job $it...")
            }

            val runningJobs = allJobs
                .filter { it.value == JobIdWithStatus.StatusEnum.RUNNING }
                .map { it.key }
                .toList()

            if (runningJobs.size > 1) {
                logger.warn("[name=${clusterSelector.name}] There are multiple running jobs")
            }

            runningJobs.forEach {
                logger.info("[name=${clusterSelector.name}] Stopping job $it...")
            }

            if (nonRunningJobs.isNotEmpty() || runningJobs.isNotEmpty()) {
                val jobs = mutableListOf<String>()
                jobs.addAll(nonRunningJobs)
                jobs.addAll(runningJobs)

                flinkClient.terminateJobs(address, jobs)

                return Result(
                    ResultStatus.OK,
                    false
                )
            } else {
                return Result(
                    ResultStatus.OK,
                    true
                )
            }
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't stop job", e)

            return Result(
                ResultStatus.ERROR,
                false
            )
        }
    }
}