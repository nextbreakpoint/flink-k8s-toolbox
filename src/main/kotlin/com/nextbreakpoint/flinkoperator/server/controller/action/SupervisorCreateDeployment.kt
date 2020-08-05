package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import io.kubernetes.client.models.V1Deployment
import org.apache.log4j.Logger

class SupervisorCreateDeployment(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<V1Deployment, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SupervisorCreateDeployment::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: V1Deployment): Result<String?> {
        return try {
            val jobOut = kubeClient.createSupervisorDeployment(clusterSelector, params)

            Result(
                ResultStatus.OK,
                jobOut.metadata.name
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't create supervisor deployment", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}