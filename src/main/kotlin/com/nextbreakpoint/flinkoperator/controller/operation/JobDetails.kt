package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import io.kubernetes.client.JSON
import org.apache.log4j.Logger

class JobDetails(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobDetails::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): OperationResult<String> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val runningJobs = flinkClient.listRunningJobs(address)

            if (runningJobs.isEmpty()) {
                logger.error("[name=${clusterSelector.name}] There is no running job")

                return OperationResult(
                    OperationStatus.ERROR,
                    "{}"
                )
            }

            if (runningJobs.size > 1) {
                logger.error("[name=${clusterSelector.name}] There are multiple jobs running")

                return OperationResult(
                    OperationStatus.ERROR,
                    "{}"
                )
            }

            val details = flinkClient.getJobDetails(address, runningJobs.first())

            return OperationResult(
                OperationStatus.OK,
                JSON().serialize(details)
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't get details of job", e)

            return OperationResult(
                OperationStatus.ERROR,
                "{}"
            )
        }
    }
}