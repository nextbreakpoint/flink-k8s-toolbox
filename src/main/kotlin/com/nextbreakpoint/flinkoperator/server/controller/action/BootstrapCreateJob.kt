package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import io.kubernetes.client.models.V1Job
import org.apache.log4j.Logger

class BootstrapCreateJob(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<V1Job, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(BootstrapCreateJob::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: V1Job): Result<String?> {
        return try {
            val jobOut = kubeClient.createBootstrapJob(clusterSelector, params)

            Result(
                ResultStatus.OK,
                jobOut.metadata.name
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't create bootstrap job", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}