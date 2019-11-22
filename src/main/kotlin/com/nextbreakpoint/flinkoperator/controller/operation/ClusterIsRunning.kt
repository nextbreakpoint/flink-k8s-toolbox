package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class ClusterIsRunning(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterIsRunning::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Boolean> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val overview = flinkClient.getOverview(address)

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
            logger.debug("[name=${clusterId.name}] Can't get overview")

            return Result(
                ResultStatus.FAILED,
                false
            )
        }
    }
}