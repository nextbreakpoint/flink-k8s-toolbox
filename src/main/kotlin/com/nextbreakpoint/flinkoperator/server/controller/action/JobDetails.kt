package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import io.kubernetes.client.JSON
import org.apache.log4j.Logger

class JobDetails(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobDetails::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): Result<String> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val runningJobs = flinkClient.listRunningJobs(address)

            if (runningJobs.isEmpty()) {
                logger.error("[name=${clusterSelector.name}] There is no running job")

                return Result(
                    ResultStatus.ERROR,
                    "{}"
                )
            }

            if (runningJobs.size > 1) {
                logger.error("[name=${clusterSelector.name}] There are multiple jobs running")

                return Result(
                    ResultStatus.ERROR,
                    "{}"
                )
            }

            val details = flinkClient.getJobDetails(address, runningJobs.first())

            return Result(
                ResultStatus.OK,
                JSON().serialize(details)
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't get details of job", e)

            return Result(
                ResultStatus.ERROR,
                "{}"
            )
        }
    }
}