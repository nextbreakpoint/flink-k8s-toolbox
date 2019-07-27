package com.nextbreakpoint.operator.command

import com.google.gson.Gson
import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskManagerId
import com.nextbreakpoint.operator.OperatorCommand

class TaskManagerDetails(flinkOptions: FlinkOptions): OperatorCommand<TaskManagerId, String>(flinkOptions) {
    override fun execute(clusterId: ClusterId, params: TaskManagerId): Result<String> {
        val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

        val details = flinkApi.getTaskManagerDetails(params.taskmanagerId)

        return Result(ResultStatus.SUCCESS, Gson().toJson(details))
    }
}