package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class PodsAreTerminated(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<Void?, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(PodsAreTerminated::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val jobmanagerPods = kubernetesContext.listJobManagerPods(clusterId)

            val taskmanagerPods = kubernetesContext.listTaskManagerPods(clusterId)

            if (jobmanagerPods.items.filter { it.status?.containerStatuses?.filter { it.lastState.running != null }?.isNotEmpty() == true }.isNotEmpty()) {
                return Result(
                    ResultStatus.AWAIT,
                    null
                )
            }

            if (taskmanagerPods.items.filter { it.status?.containerStatuses?.filter { it.lastState.running != null }?.isNotEmpty() == true }.isNotEmpty()) {
                return Result(
                    ResultStatus.AWAIT,
                    null
                )
            }

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't get pods of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}