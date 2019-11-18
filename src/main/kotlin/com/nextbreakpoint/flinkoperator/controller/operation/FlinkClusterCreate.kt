package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class FlinkClusterCreate(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : Operation<V1FlinkCluster, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(FlinkClusterCreate::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: V1FlinkCluster): Result<Void?> {
        try {
            val flinkCluster = V1FlinkCluster()
                .apiVersion("nextbreakpoint.com/v1")
                .kind("FlinkCluster")
                .metadata(params.metadata)
                .spec(params.spec)

            val response = kubernetesContext.createFlinkCluster(flinkCluster)

            if (response.statusCode == 201) {
                logger.info("Custom object created ${flinkCluster.metadata.name}")

                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            } else {
                logger.error("Can't create custom object ${flinkCluster.metadata.name}")

                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("Can't create cluster resource ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}