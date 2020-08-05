package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class ArePodsTerminated(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ArePodsTerminated::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): Result<Boolean> {
        try {
            val jobmanagerPods = kubeClient.listJobManagerPods(clusterSelector)

            val taskmanagerPods = kubeClient.listTaskManagerPods(clusterSelector)

            val jobmanagerRunning = jobmanagerPods.items.filter {
                it.status?.containerStatuses?.filter {
                    it.state.running != null
                }?.isNotEmpty() == true
            }.isNotEmpty()

            val taskmanagerRunning = taskmanagerPods.items.filter {
                it.status?.containerStatuses?.filter {
                    it.state.running != null
                }?.isNotEmpty() == true
            }.isNotEmpty()

            return Result(
                ResultStatus.OK,
                !jobmanagerRunning && !taskmanagerRunning
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't get pods", e)

            return Result(
                ResultStatus.ERROR,
                false
            )
        }
    }
}