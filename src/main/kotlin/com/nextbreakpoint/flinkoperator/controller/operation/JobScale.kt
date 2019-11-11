package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class JobScale(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<Int, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(JobScale::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Int): Result<Void?> {
        try {
            val address = kubernetesContext.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            flinkContext.triggerJobRescaling(address, params)

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't rescale job of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}