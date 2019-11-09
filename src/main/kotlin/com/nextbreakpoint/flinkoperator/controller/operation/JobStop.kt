package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class JobStop(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<Void?, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(JobStop::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val address = kubernetesContext.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkContext.listRunningJobs(address)

            if (runningJobs.size > 1) {
                logger.warn("There are multiple jobs running in cluster ${clusterId.name}")
            }

            if (runningJobs.isEmpty()) {
                logger.warn("There is no running job in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            }

            flinkContext.terminateJobs(address, runningJobs)

            val stillRunningJobs = flinkContext.listRunningJobs(address)

            if (stillRunningJobs.isNotEmpty()) {
                return Result(
                    ResultStatus.AWAIT,
                    null
                )
            }

            logger.info("Job of cluster ${clusterId.name} has been stopped")

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't stop job of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}