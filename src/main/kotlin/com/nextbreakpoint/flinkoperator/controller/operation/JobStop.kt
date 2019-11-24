package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class JobStop(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobStop::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkClient.listRunningJobs(address)

            if (runningJobs.size > 1) {
                logger.warn("[name=${clusterId.name}] There are multiple jobs running")
            }

            if (runningJobs.isEmpty()) {
                logger.warn("[name=${clusterId.name}] Job already stopped!")

                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            }

            flinkClient.terminateJobs(address, runningJobs)

            val stillRunningJobs = flinkClient.listRunningJobs(address)

            if (stillRunningJobs.isNotEmpty()) {
                return Result(
                    ResultStatus.AWAIT,
                    null
                )
            }

            logger.debug("[name=${clusterId.name}] Job stopped")

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't stop job", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}