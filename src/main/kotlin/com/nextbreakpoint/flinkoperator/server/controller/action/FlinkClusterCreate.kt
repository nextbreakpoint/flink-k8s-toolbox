package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class FlinkClusterCreate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<V1FlinkCluster, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(FlinkClusterCreate::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: V1FlinkCluster): Result<Void?> {
        return try {
            val flinkCluster = V1FlinkCluster()
                .apiVersion("nextbreakpoint.com/v1")
                .kind("FlinkCluster")
                .metadata(params.metadata)
                .spec(params.spec)

            val response = kubeClient.createFlinkCluster(flinkCluster)

            if (response.statusCode == 201) {
                logger.info("[name=${clusterSelector.name}] Custom object created")

                Result(
                    ResultStatus.OK,
                    null
                )
            } else {
                logger.error("[name=${clusterSelector.name}] Can't create custom object")

                Result(
                    ResultStatus.ERROR,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't create cluster resource", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}