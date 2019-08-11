package com.nextbreakpoint.flinkoperator.controller.command

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.utils.FlinkServerUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class TaskManagerDetails(flinkOptions: FlinkOptions): OperatorCommand<TaskManagerId, String>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(TaskManagerDetails::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: TaskManagerId): Result<String> {
        try {
            val flinkApi = FlinkServerUtils.find(flinkOptions, clusterId.namespace, clusterId.name)

            val details = flinkApi.getTaskManagerDetails(params.taskmanagerId)

            return Result(
                ResultStatus.SUCCESS,
                Gson().toJson(details)
            )
        } catch (e : Exception) {
            logger.error("Can't get details of TaskManager of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                "{}"
            )
        }
    }
}