package com.nextbreakpoint.operator.command

import com.google.gson.Gson
import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand

class TaskManagersList(flinkOptions: FlinkOptions) : OperatorCommand<Void?, String>(flinkOptions) {
    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
        val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

        val taskmanagers = flinkApi.taskManagersOverview.taskmanagers.toList()

        return Result(ResultStatus.SUCCESS, Gson().toJson(taskmanagers))
    }
}