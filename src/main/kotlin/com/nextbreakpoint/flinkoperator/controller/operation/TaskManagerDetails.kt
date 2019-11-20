package com.nextbreakpoint.flinkoperator.controller.operation

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class TaskManagerDetails(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient): Operation<TaskManagerId, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagerDetails::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: TaskManagerId): Result<String> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val details = flinkClient.getTaskManagerDetails(address, params)

            return Result(
                ResultStatus.SUCCESS,
                Gson().toJson(details)
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't get details of TaskManager", e)

            return Result(
                ResultStatus.FAILED,
                "{}"
            )
        }
    }
}