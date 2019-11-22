package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class JobHasStarted(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobHasStarted::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkClient.listRunningJobs(address)

            if (runningJobs.isEmpty()) {
                logger.debug("[name=${clusterId.name}] Can't find a running job")

                return Result(
                    ResultStatus.AWAIT,
                    null
                )
            }

            if (runningJobs.size > 1) {
                logger.warn("[name=${clusterId.name}] There are multiple jobs running")
            }

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.warn("[name=${clusterId.name}] Can't get list of jobs")

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}