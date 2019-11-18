package com.nextbreakpoint.flinkoperator.controller.operation

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.TaskOperation
import org.apache.log4j.Logger

class TaskManagerDetails(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext): TaskOperation<TaskManagerId, String>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(TaskManagerDetails::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: TaskManagerId): Result<String> {
        try {
            val address = kubernetesContext.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val details = flinkContext.getTaskManagerDetails(address, params)

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