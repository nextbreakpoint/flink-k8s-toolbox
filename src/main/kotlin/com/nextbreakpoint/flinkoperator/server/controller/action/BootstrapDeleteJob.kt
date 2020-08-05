package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class BootstrapDeleteJob(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(BootstrapDeleteJob::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): Result<Void?> {
        return try {
            kubeClient.deleteBootstrapJob(clusterSelector)

            kubeClient.deleteBootstrapPod(clusterSelector)

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't delete bootstrap job", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}