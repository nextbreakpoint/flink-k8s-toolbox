package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class ClusterIsRunning(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<Void?, Boolean>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(ClusterIsRunning::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Boolean> {
        try {
            val address = kubernetesContext.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val overview = flinkContext.getOverview(address)

            if (overview.slotsTotal > 0 && overview.taskmanagers > 0 && (overview.jobsRunning == 1 || (overview.jobsRunning == 0 && overview.jobsFinished >= 1))) {
                return Result(
                    ResultStatus.SUCCESS,
                    overview.jobsRunning == 0 && overview.jobsFinished >= 1
                )
            } else {
                return Result(
                    ResultStatus.AWAIT,
                    false
                )
            }
        } catch (e : Exception) {
            logger.warn("Can't get overview of cluster ${clusterId.name}")

            return Result(
                ResultStatus.FAILED,
                false
            )
        }
    }
}