package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import io.kubernetes.client.models.V1Service
import org.apache.log4j.Logger

class ClusterCreateService(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<V1Service, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreateService::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: V1Service): Result<String?> {
        return try {
            val serviceOut = kubeClient.createService(clusterSelector, params)

            Result(
                ResultStatus.OK,
                serviceOut.metadata.name
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't create service", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}