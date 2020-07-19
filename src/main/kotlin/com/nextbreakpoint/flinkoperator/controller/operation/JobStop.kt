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

class JobStop(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobStop::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): OperationResult<Void?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val nonRunningJobs = flinkClient.listJobs(address, setOf(
                JobIdWithStatus.StatusEnum.SUSPENDING,
                JobIdWithStatus.StatusEnum.RESTARTING,
                JobIdWithStatus.StatusEnum.RECONCILING,
                JobIdWithStatus.StatusEnum.SUSPENDED,
                JobIdWithStatus.StatusEnum.CREATED
            ))

            nonRunningJobs.forEach {
                logger.info("[name=${clusterSelector.name}] Stopping job $it...")
            }

            if (nonRunningJobs.isNotEmpty()) {
                flinkClient.terminateJobs(address, nonRunningJobs)
            }

            val runningJobs = flinkClient.listRunningJobs(address)

            if (runningJobs.size > 1) {
                logger.warn("[name=${clusterSelector.name}] There are multiple jobs running")
            }

            if (runningJobs.isEmpty()) {
                logger.warn("[name=${clusterSelector.name}] Job already stopped!")

                return OperationResult(
                    OperationStatus.OK,
                    null
                )
            }

            flinkClient.terminateJobs(address, runningJobs)

            val stillRunningJobs = flinkClient.listRunningJobs(address)

            if (stillRunningJobs.isNotEmpty()) {
                logger.debug("[name=${clusterSelector.name}] Job still running...")

                return OperationResult(
                    OperationStatus.ERROR,
                    null
                )
            }

            logger.debug("[name=${clusterSelector.name}] Job stopped")

            return OperationResult(
                OperationStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't stop job", e)

            return OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}