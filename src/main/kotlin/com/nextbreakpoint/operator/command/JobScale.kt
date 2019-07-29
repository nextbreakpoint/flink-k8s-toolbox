package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class JobScale(flinkOptions: FlinkOptions) : OperatorCommand<Void?, String>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(JobScale::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
//        val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

//        val operation = flinkApi.triggerJobRescaling(scaleParams.jobId, scaleParams.parallelism)
//
//        return "{\"status\":\"SUCCESS\",\"requestId\":\"${operation.requestId}\"}"

        return Result(ResultStatus.FAILED, "{}")
    }
}