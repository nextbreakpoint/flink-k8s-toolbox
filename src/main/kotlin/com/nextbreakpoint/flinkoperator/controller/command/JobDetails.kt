package com.nextbreakpoint.flinkoperator.controller.command

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class JobDetails(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<Void?, String>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(JobDetails::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
        try {
            val address = kubernetesContext.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkContext.listRunningJobs(address)

            if (runningJobs.isEmpty()) {
                logger.error("There is no running job in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.FAILED,
                    "{}"
                )
            }

            if (runningJobs.size > 1) {
                logger.error("There are multiple jobs running in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.FAILED,
                    "{}"
                )
            }

            val details = flinkContext.getJobDetails(address, runningJobs.first())

            return Result(
                ResultStatus.SUCCESS,
                Gson().toJson(details)
            )
        } catch (e : Exception) {
            logger.error("Can't get details of job of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                "{}"
            )
        }
    }
}