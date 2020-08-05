package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class JobIsFinished(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobIsFinished::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): Result<Boolean> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val overview = flinkClient.getOverview(address)

            if (overview.jobsRunning == 0 && overview.jobsFinished >= 1) {
                Result(
                    ResultStatus.OK,
                    true
                )
            } else {
                Result(
                    ResultStatus.OK,
                    false
                )
            }
        } catch (e : Exception) {
            logger.warn("[name=${clusterSelector.name}] Can't get overview")

            Result(
                ResultStatus.ERROR,
                false
            )
        }
    }
}