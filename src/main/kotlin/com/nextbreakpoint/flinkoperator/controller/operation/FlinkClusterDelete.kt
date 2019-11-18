package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class FlinkClusterDelete(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : Operation<Void?, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(FlinkClusterDelete::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val response = kubernetesContext.deleteFlinkCluster(clusterId)

            if (response.statusCode == 200) {
                logger.info("Custom object deleted ${clusterId.name}")

                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            } else {
                logger.error("Can't delete custom object ${clusterId.name}")

                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("Can't delete cluster resource ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}