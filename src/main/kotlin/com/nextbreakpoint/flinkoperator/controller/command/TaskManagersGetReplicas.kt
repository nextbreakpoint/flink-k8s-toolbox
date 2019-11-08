package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class TaskManagersGetReplicas(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<Void?, Int>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersGetReplicas::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Int> {
        try {
            val replicas = kubernetesContext.getTaskManagerStatefulSetReplicas(clusterId)

            return Result(
                ResultStatus.SUCCESS,
                replicas
            )
        } catch (e : Exception) {
            logger.error("Can't get replicas of task managers of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                0
            )
        }
    }
}