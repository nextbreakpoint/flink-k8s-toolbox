package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import io.kubernetes.client.models.V1Job
import org.apache.log4j.Logger

class BootstrapCreateJob(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<V1Job, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(BootstrapCreateJob::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: V1Job): Result<Void?> {
        try {
            logger.info("[name=${clusterId.name}] Creating bootstrap job...")

            val jobs = kubeClient.listBootstrapJobs(clusterId)

            if (jobs.items.isNotEmpty()) {
                kubeClient.deleteBootstrapJobs(clusterId)

                val jobOut = kubeClient.createBootstrapJob(clusterId, params)

                logger.info("[name=${clusterId.name}] Bootstrap job recreated: ${jobOut.metadata.name}")
            } else {
                val jobOut = kubeClient.createBootstrapJob(clusterId, params)

                logger.info("[name=${clusterId.name}] Bootstrap job created: ${jobOut.metadata.name}")
            }

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't create bootstrap job", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}