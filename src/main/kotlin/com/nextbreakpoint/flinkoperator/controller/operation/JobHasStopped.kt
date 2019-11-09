package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class JobHasStopped(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<Void?, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(JobHasStopped::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val address = kubernetesContext.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkContext.listRunningJobs(address)

            if (runningJobs.size > 1) {
                logger.warn("There are multiple jobs running in cluster ${clusterId.name}")
            }

            if (runningJobs.isNotEmpty()) {
                logger.info("There is still a running job in cluster ${clusterId.name}")

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
            logger.warn("Can't get list of jobs for cluster ${clusterId.name}")

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}