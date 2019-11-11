package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import org.apache.log4j.Logger

class CreateBootstrapJob(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<ClusterResources, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(CreateBootstrapJob::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): Result<Void?> {
        try {
            val jobs = kubernetesContext.listBootstrapJobs(clusterId)

            if (jobs.items.isEmpty()) {
                logger.info("Creating bootstrap Job of cluster ${clusterId.name}...")

                val jobOut = kubernetesContext.createBootstrapJob(clusterId, params)

                logger.info("Bootstrap job of cluster ${clusterId.name} created with name ${jobOut.metadata.name}")

                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            } else {
                logger.warn("Bootstrap job of cluster ${clusterId.name} already exists")

                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("Can't create bootstrap job of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}