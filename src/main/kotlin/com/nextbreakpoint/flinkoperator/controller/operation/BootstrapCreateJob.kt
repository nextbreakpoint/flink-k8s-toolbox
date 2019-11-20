package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import org.apache.log4j.Logger

class BootstrapCreateJob(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<ClusterResources, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(BootstrapCreateJob::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): Result<Void?> {
        try {
            val jobs = kubeClient.listBootstrapJobs(clusterId)

            if (jobs.items.isEmpty()) {
                logger.info("[name=${clusterId.name}] Creating bootstrap job...")

                val jobOut = kubeClient.createBootstrapJob(clusterId, params)

                logger.info("[name=${clusterId.name}] Bootstrap job created with name ${jobOut.metadata.name}")

                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            } else {
                logger.warn("[name=${clusterId.name}] Bootstrap job already exists")

                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't create bootstrap job", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}