package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class ClusterIsRunning(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterIsRunning::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): OperationResult<Boolean> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val overview = flinkClient.getOverview(address)

            if (overview.slotsTotal > 0 && overview.taskmanagers > 0 && (overview.jobsRunning == 1 || (overview.jobsRunning == 0 && overview.jobsFinished >= 1))) {
                return OperationResult(
                    OperationStatus.COMPLETED,
                    overview.jobsRunning == 0 && overview.jobsFinished >= 1
                )
            } else {
                return OperationResult(
                    OperationStatus.RETRY,
                    false
                )
            }
        } catch (e : Exception) {
            logger.debug("[name=${clusterId.name}] Can't get overview")

            return OperationResult(
                OperationStatus.FAILED,
                false
            )
        }
    }
}