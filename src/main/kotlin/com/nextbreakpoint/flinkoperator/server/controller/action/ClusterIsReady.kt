package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.ClusterScale
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class ClusterIsReady(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<ClusterScale, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterIsReady::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: ClusterScale): Result<Boolean> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val overview = flinkClient.getOverview(address)

            if (overview.slotsAvailable >= params.taskManagers * params.taskSlots && overview.taskmanagers >= params.taskManagers) {
                Result(
                    ResultStatus.OK,
                    true
                )
            } else {
                Result(
                    ResultStatus.OK,
                    false
                )
            }
        } catch (e : Exception) {
            logger.debug("[name=${clusterSelector.name}] Can't get overview")

            Result(
                ResultStatus.ERROR,
                false
            )
        }
    }
}