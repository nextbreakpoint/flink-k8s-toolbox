package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class JobScale(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<Void?, String>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(JobScale::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
//        val flinkApi = FlinkServerUtils.find(flinkOptions, clusterId.namespace, clusterId.name)

//        val operation = flinkApi.triggerJobRescaling(scaleParams.jobId, scaleParams.parallelism)
//
//        return "{\"status\":\"SUCCESS\",\"requestId\":\"${operation.requestId}\"}"

        return Result(
            ResultStatus.FAILED,
            "{}"
        )
    }
}