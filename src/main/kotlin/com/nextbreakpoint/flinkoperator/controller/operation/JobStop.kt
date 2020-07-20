package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkclient.model.JobIdWithStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class JobStop(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Boolean>(flinkOptions, flinkClient, kubeClient) {
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

    override fun execute(clusterSelector: ClusterSelector, params: Void?): OperationResult<Boolean> {
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

                return OperationResult(
                    OperationStatus.OK,
                    false
                )
            } else {
                return OperationResult(
                    OperationStatus.OK,
                    true
                )
            }
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't stop job", e)

            return OperationResult(
                OperationStatus.ERROR,
                false
            )
        }
    }
}