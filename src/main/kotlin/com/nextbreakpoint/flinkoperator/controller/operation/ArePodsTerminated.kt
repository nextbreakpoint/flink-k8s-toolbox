package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class ArePodsTerminated(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ArePodsTerminated::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): OperationResult<Boolean> {
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

            return OperationResult(
                OperationStatus.OK,
                !jobmanagerRunning && !taskmanagerRunning
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't get pods", e)

            return OperationResult(
                OperationStatus.ERROR,
                false
            )
        }
    }
}