package com.nextbreakpoint.flinkoperator.controller.operation

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class TaskManagersList(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<Void?, String>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersList::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
        try {
            val address = kubernetesContext.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val overview = flinkContext.getTaskManagersOverview(address)

            return Result(
                ResultStatus.SUCCESS,
                Gson().toJson(overview.taskmanagers)
            )
        } catch (e : Exception) {
            logger.error("Can't get list of TaskManagers of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                "{}"
            )
        }
    }
}