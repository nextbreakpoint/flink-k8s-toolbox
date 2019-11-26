package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import org.apache.log4j.Logger

class PodsAreTerminated(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(PodsAreTerminated::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): OperationResult<Void?> {
        try {
            val jobmanagerPods = kubeClient.listJobManagerPods(clusterId)

            val taskmanagerPods = kubeClient.listTaskManagerPods(clusterId)

            if (jobmanagerPods.items.filter { it.status?.containerStatuses?.filter { it.lastState.running != null }?.isNotEmpty() == true }.isNotEmpty()) {
                return OperationResult(
                    OperationStatus.RETRY,
                    null
                )
            }

            if (taskmanagerPods.items.filter { it.status?.containerStatuses?.filter { it.lastState.running != null }?.isNotEmpty() == true }.isNotEmpty()) {
                return OperationResult(
                    OperationStatus.RETRY,
                    null
                )
            }

            return OperationResult(
                OperationStatus.COMPLETED,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't get pods", e)

            return OperationResult(
                OperationStatus.FAILED,
                null
            )
        }
    }
}