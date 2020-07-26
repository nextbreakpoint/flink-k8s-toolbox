package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class FlinkClusterCreate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<V1FlinkCluster, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(FlinkClusterCreate::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: V1FlinkCluster): OperationResult<Void?> {
        return try {
            val flinkCluster = V1FlinkCluster()
                .apiVersion("nextbreakpoint.com/v1")
                .kind("FlinkCluster")
                .metadata(params.metadata)
                .spec(params.spec)

            val response = kubeClient.createFlinkCluster(flinkCluster)

            if (response.statusCode == 201) {
                logger.info("[name=${clusterSelector.name}] Custom object created")

                OperationResult(
                    OperationStatus.OK,
                    null
                )
            } else {
                logger.error("[name=${clusterSelector.name}] Can't create custom object")

                OperationResult(
                    OperationStatus.ERROR,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't create cluster resource", e)

            OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}