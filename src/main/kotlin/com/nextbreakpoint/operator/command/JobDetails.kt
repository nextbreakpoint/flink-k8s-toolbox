package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand

class JobDetails(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

//        val details = flinkApi.getJobDetails(jobDescriptor.jobId)

//        return Gson().toJson(details)

        return Result(ResultStatus.SUCCESS, null)
    }
}