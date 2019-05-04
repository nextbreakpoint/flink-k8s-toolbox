package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class JobScale(flinkOptions: FlinkOptions) : OperatorCommand<Void?, String>(flinkOptions) {
    private val logger = Logger.getLogger(JobScale::class.simpleName)

    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
        val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

//        logger.info("Rescaling job...")
//
//        val operation = flinkApi.triggerJobRescaling(scaleParams.jobId, scaleParams.parallelism)
//
//        logger.info("done")

//        return "{\"status\":\"SUCCESS\",\"requestId\":\"${operation.requestId}\"}"
        return Result(ResultStatus.SUCCESS, "{}")
    }
}